# IPDPS '25 — Optimizing Fine-Grained Parallelism Through Dynamic Load Balancing on Multi-Socket Many-Core Systems

Source modifications and patch for the paper:

> **Optimizing Fine-Grained Parallelism Through Dynamic Load Balancing on Multi-Socket Many-Core Systems**
> Wenyi Wang, Maxime Gonthier, Poornima Nookala, Haochen Pan, Ian Foster, Ioan Raicu, Kyle Chard
> *IEEE IPDPS 2025 (39th International Parallel and Distributed Processing Symposium), Milano, Italy*
> ArXiv: https://arxiv.org/abs/2502.05293
> IEEE: https://ieeexplore.ieee.org/document/11078401

---

## What's in this branch

This branch contains **only the files modified from GCC 12 (releases/gcc-12, snapshot 2023-02-07)**,
keeping the original directory structure so they can be dropped into or patched onto a vanilla GCC 12 tree.

```
gcc/               Compiler-side OpenMP changes
libgomp/           Core runtime changes (XQueue, distributed barrier, NUMA load balancing)
libsanitizer/      One-line build fix for the test environment
benchmark/         Benchmarks (to be added)
ipdps-25.patch     Unified diff against GCC 12 base (see below)
```

### Key contributions in `libgomp/`

| File | Change |
|------|--------|
| `task.c` | XQueue — lock-free concurrent task queue replacing GNU's global priority queue |
| `team.c` | NUMA-aware thread team management; distributed tree barrier init |
| `config/linux/bar.c` / `bar.h` | Distributed tree barrier replacing the centralized futex barrier |
| `config/posix/simple-bar.h` | Posix barrier simplification |
| `libgomp.h` | New data structures for XQueue and load balancing |
| `parallel.c` | Hooks for redirect-push and work-stealing load balancing |
| `taskloop.c`, `env.c`, `error.c`, `omp.h.in`, `libgomp.map` | Supporting changes |

---

## Applying the patch to your own GCC 12 tree

The patch is generated against commit `6f23c9077feebb29c2a28ffe89b287286df27d6d`
on the `releases/gcc-12` branch of the GCC mirror (snapshotted ~2023-02-07,
`gcc/DATESTAMP` = 20230207).

```bash
# 1. Clone or check out GCC 12
git clone https://github.com/gcc-mirror/gcc.git
cd gcc
git checkout releases/gcc-12
git checkout 6f23c9077feebb29c2a28ffe89b287286df27d6d

# 2. Apply the patch
git apply /path/to/ipdps-25.patch

# 3. Build (in a separate build directory)
mkdir build && cd build
../configure --prefix=$HOME/gcc12-xtask --enable-languages=c,c++ \
             --disable-multilib --disable-bootstrap
make -j$(nproc) all-target-libgomp
make install-target-libgomp
```

---

## Full history

The complete commit history (including GNU GCC upstream history) is available on the
`xtask` branch of this repository.
