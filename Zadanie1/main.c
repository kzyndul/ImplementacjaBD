#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <errno.h>

#include "err.h"

// #define BLOCK_SIZE (8 * 1024 * 1024) // 8MB
#define BLOCK_SIZE (4096) // 4KB
#define CRC64_ECMA182_POLY 0x42F0E1EBA9EA3693ULL

static uint64_t crc64_table[256] = {0};


static void generate_crc64_table(void)
{
	uint64_t i, j, c, crc;
	for (i = 0; i < 256; i++) {
		crc = 0;
		c = i << 56;
		for (j = 0; j < 8; j++) {
			if ((crc ^ c) & 0x8000000000000000ULL)
				crc = (crc << 1) ^ CRC64_ECMA182_POLY;
			else
				crc <<= 1;
			c <<= 1;
		}
		crc64_table[i] = crc;
	}
}

uint64_t crc64_be(uint64_t crc, const void *p, size_t len)
{
	size_t i, t;
	const unsigned char *_p = p;
	for (i = 0; i < len; i++) {
		t = ((crc >> 56) ^ (*_p++)) & 0xFF;
		crc = crc64_table[t] ^ (crc << 8);
	}
	return crc;
}


double get_time_diff(struct timespec start, struct timespec end) {
    return (end.tv_sec - start.tv_sec) + (end.tv_nsec - start.tv_nsec) / 1e9;
}

uint64_t sequential_read(const char *filename, double *time_taken) {
    struct timespec start, end;
    int fd;
    unsigned char *buffer;
    ssize_t bytes_read;
    uint64_t crc = 0;
    
    buffer = malloc(BLOCK_SIZE);
    ASSERT_NOT_NULL(buffer);
    
    fd = open(filename, O_RDONLY);
    
    ASSERT_SYS_OK(fd);


    ASSERT_ZERO(clock_gettime(CLOCK_MONOTONIC, &start));

    while ((bytes_read = read(fd, buffer, BLOCK_SIZE)) > 0) {
        crc = crc64_be(crc, buffer, bytes_read);
    }
    
    ASSERT_ZERO(clock_gettime(CLOCK_MONOTONIC, &end)); // CLOCK_REALTIME
    

    ASSERT_SYS_OK(bytes_read);

    
    *time_taken = get_time_diff(start, end);

    ASSERT_SYS_OK(close(fd));
    free(buffer);
    
    return crc;
}

uint64_t random_read(const char *filename, double *time_taken) {
    struct timespec start, end;
    int fd;
    unsigned char *buffer;
    ssize_t bytes_read;
    uint64_t crc = 0;
    struct stat st;
    off_t file_size;
    off_t front_offset = 0, back_offset;
    int read_from_front = 1;
    

    buffer = malloc(BLOCK_SIZE);
    ASSERT_NOT_NULL(buffer);
        
    fd = open(filename, O_RDONLY);

    ASSERT_SYS_OK(fd);
    ASSERT_ZERO(fstat(fd, &st));
    
    file_size = st.st_size;
    back_offset = file_size;

    ASSERT_ZERO(clock_gettime(CLOCK_MONOTONIC, &start));

    while (front_offset < back_offset) {
        if (read_from_front) {
            lseek(fd, front_offset, SEEK_SET);
            size_t to_read = (front_offset + BLOCK_SIZE <= back_offset) ? 
                            BLOCK_SIZE : (back_offset - front_offset);
            bytes_read = read(fd, buffer, to_read);
            if (bytes_read > 0) {
                crc = crc64_be(crc, buffer, bytes_read);
                front_offset += bytes_read;
            }
        } else {
            size_t to_read = (back_offset - front_offset >= BLOCK_SIZE) ? 
                            BLOCK_SIZE : (back_offset - front_offset);
            back_offset -= to_read;
            lseek(fd, back_offset, SEEK_SET);
            bytes_read = read(fd, buffer, to_read);
            if (bytes_read > 0) {
                crc = crc64_be(crc, buffer, bytes_read);
            }
        }
        
        if (bytes_read <= 0) break;
        read_from_front = !read_from_front;
    }

    ASSERT_ZERO(clock_gettime(CLOCK_MONOTONIC, &end));

    *time_taken = get_time_diff(start, end);
    
    ASSERT_SYS_OK(close(fd));
    free(buffer);
    
    return crc;
}

uint64_t sequential_mmap(const char *filename, double *time_taken) {
    struct timespec start, end;
    int fd;
    struct stat st;
    off_t file_size;
    uint64_t crc = 0;
        
    fd = open(filename, O_RDONLY);

    ASSERT_SYS_OK(fd);
    ASSERT_ZERO(fstat(fd, &st));
    
    file_size = st.st_size;
    ASSERT_ZERO(clock_gettime(CLOCK_MONOTONIC, &start));

    off_t offset = 0;
    while (offset < file_size) {
        size_t map_size = (offset + BLOCK_SIZE <= file_size) ? 
                         BLOCK_SIZE : (file_size - offset);
        
        void *mapped = mmap(NULL, map_size, PROT_READ, MAP_PRIVATE, fd, offset);
        if (mapped == MAP_FAILED) {
            perror("mmap");
            break;
        }
        
        crc = crc64_be(crc, (unsigned char *)mapped, map_size);
        
        ASSERT_SYS_OK(munmap(mapped, map_size));
        offset += map_size;
    }

    ASSERT_ZERO(clock_gettime(CLOCK_MONOTONIC, &end));

    *time_taken = get_time_diff(start, end);

    ASSERT_SYS_OK(close(fd));

    return crc;
}

uint64_t random_mmap(const char *filename, double *time_taken) {
    struct timespec start, end;
    int fd;
    struct stat st;
    off_t file_size;
    uint64_t crc = 0;
    
    fd = open(filename, O_RDONLY);

    ASSERT_SYS_OK(fd);
    ASSERT_ZERO(fstat(fd, &st));
    
    file_size = st.st_size;
    ASSERT_ZERO(clock_gettime(CLOCK_MONOTONIC, &start));
    
    off_t front_offset = 0, back_offset = file_size;
    int read_from_front = 1;
    
    while (front_offset < back_offset) {
        off_t offset;
        size_t map_size;
        
        if (read_from_front) {
            offset = front_offset;
            map_size = (front_offset + BLOCK_SIZE <= back_offset) ? 
                      BLOCK_SIZE : (back_offset - front_offset);
            front_offset += map_size;
        } else {
            map_size = (back_offset - front_offset >= BLOCK_SIZE) ? 
                      BLOCK_SIZE : (back_offset - front_offset);
            back_offset -= map_size;
            offset = back_offset;
        }
        
        void *mapped = mmap(NULL, map_size, PROT_READ, MAP_PRIVATE, fd, offset);
        if (mapped == MAP_FAILED) {
            perror("mmap");
            break;
        }
        
        crc = crc64_be(crc, (unsigned char *)mapped, map_size);

        ASSERT_SYS_OK(munmap(mapped, map_size));
        read_from_front = !read_from_front;
    }

    ASSERT_ZERO(clock_gettime(CLOCK_MONOTONIC, &end));

    *time_taken = get_time_diff(start, end);

    ASSERT_SYS_OK(close(fd));

    return crc;
}

int main(int argc, char *argv[]) {
    if (argc != 2) {
        fprintf(stderr, "Użycie: %s <nazwa_pliku>\n", argv[0]);
        return 1;
    }

    generate_crc64_table();
    
    const char *filename = argv[1];
    double time_seq_read, time_rand_read, time_seq_mmap, time_rand_mmap;
    uint64_t crc_seq_read, crc_rand_read, crc_seq_mmap, crc_rand_mmap;
    
    printf("Plik: %s\n", filename);
    // printf("Rozmiar bloku: %d MB\n\n", BLOCK_SIZE / (1024 * 1024));
    
    // 1. Sekwencyjny read()
    printf("1. Sekwencyjny odczyt (read)...\n");
    crc_seq_read = sequential_read(filename, &time_seq_read);
    printf("   Czas: %.6f s\n", time_seq_read);
    printf("   CRC64: 0x%016lX\n\n", crc_seq_read);
    
    // 2. Losowy read()
    printf("2. Losowy odczyt (read)...\n");
    crc_rand_read = random_read(filename, &time_rand_read);
    printf("   Czas: %.6f s\n", time_rand_read);
    printf("   CRC64: 0x%016lX\n\n", crc_rand_read);
    
    // 3. Sekwencyjny mmap()
    printf("3. Sekwencyjny odczyt (mmap)...\n");
    crc_seq_mmap = sequential_mmap(filename, &time_seq_mmap);
    printf("   Czas: %.6f s\n", time_seq_mmap);
    printf("   CRC64: 0x%016lX\n\n", crc_seq_mmap);
    
    // 4. Losowy mmap()
    printf("4. Losowy odczyt (mmap)...\n");
    crc_rand_mmap = random_mmap(filename, &time_rand_mmap);
    printf("   Czas: %.6f s\n", time_rand_mmap);
    printf("   CRC64: 0x%016lX\n\n", crc_rand_mmap);
    
    // Podsumowanie
    // printf("=== PODSUMOWANIE ===\n");
    // printf("Wszystkie sumy CRC64 %s\n", 
    //        (crc_seq_read == crc_rand_read && 
    //         crc_rand_read == crc_seq_mmap && 
    //         crc_seq_mmap == crc_rand_mmap) ? "SĄ ZGODNE ✓" : "NIE SĄ ZGODNE ✗");
    
    return 0;
}