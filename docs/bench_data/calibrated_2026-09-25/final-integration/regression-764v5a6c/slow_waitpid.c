
#define _GNU_SOURCE
#include <dlfcn.h>
#include <stdio.h>
#include <stdatomic.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
pid_t waitpid(pid_t pid, int *status, int options) {
    static atomic_int delayed = 0;
    pid_t (*real_waitpid)(pid_t, int *, int) = dlsym(RTLD_NEXT, "waitpid");
    char command[4096] = {0};
    FILE *file = fopen("/proc/self/cmdline", "rb");
    if (file) {
        size_t size = fread(command, 1, sizeof(command) - 1, file);
        fclose(file);
        for (size_t i = 0; i < size; ++i) if (!command[i]) command[i] = ' ';
        if (strstr(command, "--internal-coordinator") && !atomic_exchange(&delayed, 1)) {
            struct timespec delay = {.tv_sec = 0, .tv_nsec = 400000000};
            while (nanosleep(&delay, &delay)) {}
        }
    }
    return real_waitpid(pid, status, options);
}
