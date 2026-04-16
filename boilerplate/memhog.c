#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
int main() {
    size_t chunk = 1024 * 1024;
    int total = 0;
    while (1) {
        void *p = malloc(chunk);
        if (!p) break;
        memset(p, 1, chunk);
        total++;
        printf("Allocated %d MiB\n", total);
        fflush(stdout);
        sleep(1);
    }
}
