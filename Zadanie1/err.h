#ifndef MIM_ERR_H
#define MIM_ERR_H

#include <stdnoreturn.h>


/* Assert that expression evaluates to zero (otherwise use result as error number, as in pthreads). */
#define ASSERT_ZERO(expr)                                                                   \
    do {                                                                                    \
        int err_code = (expr);                                                                 \
        if (err_code != 0)                                                                     \
            syserr(                                                                         \
                "Failed: %s\n\tIn function %s() in %s line %d.\n\tErrno: ",                 \
                #expr, __func__, __FILE__, __LINE__                                         \
            );                                                                              \
    } while(0)


#define ASSERT_NOT_NULL(ptr)                                                                \
do {                                                                                        \
    if ((ptr) == NULL)                                                                      \
        syserr("Allocation failed: %s\n\tIn function %s() in %s line %d.",                  \
               #ptr, __func__, __FILE__, __LINE__                                           \
        );                                                                                  \
    } while(0)

/*
    Assert that expression doesn't evaluate to -1 .
*/
#define ASSERT_SYS_OK(expr)                                                                \
    do {                                                                                   \
        if ((expr) == -1)                                                                  \
            syserr(                                                                        \
                "System command failed: %s\n\tIn function %s() in %s line %d.\n\tErrno: ", \
                #expr, __func__, __FILE__, __LINE__                                        \
            );                                                                             \
    } while(0)

/* Prints with information about system error (errno) and quits. */
_Noreturn extern void syserr(const char* fmt, ...);

#endif
