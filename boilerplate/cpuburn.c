#include <stdio.h>
#include <time.h>
int main() {
    struct timespec start, end;
    clock_gettime(CLOCK_MONOTONIC, &start);
    volatile long long x = 0;
    for (long long i = 0; i < 2000000000LL; i++) x += i;
    clock_gettime(CLOCK_MONOTONIC, &end);
    double elapsed = (end.tv_sec - start.tv_sec) + (end.tv_nsec - start.tv_nsec)/1e9;
    printf("Done in %.2f seconds\n", elapsed);
}
