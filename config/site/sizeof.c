#include <stdio.h>

int main(void) {
    printf("ac_cv_sizeof_char=%d\n", sizeof(char));
    printf("ac_cv_sizeof_short=%d\n", sizeof(short));
    printf("ac_cv_sizeof_int=%d\n", sizeof(int));
    printf("ac_cv_sizeof_long=%d\n", sizeof(long));
    printf("ac_cv_sizeof_long_long=%d\n", sizeof(long long));
    printf("ac_cv_sizeof_double=%d\n", sizeof(double));
    printf("ac_cv_sizeof_char_p=%d\n", sizeof(char *));
}
