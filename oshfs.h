#ifndef FS_H
#define FS_H

#define FS_SIZE (4 * 1024 * 1024 * 1024ULL) 
#define BLOCK_SIZE (4 * 1024)
#define BLOCK_NUM (FS_SIZE / BLOCK_SIZE)
#define BMBLOCK_NUM (BLOCK_NUM / 8 / BLOCK_SIZE)
#define IBLOCK_NUM 2
#define CONTENT_SIZE (BLOCK_SIZE - sizeof(ull))
#define ULL_NUM BLOCK_SIZE / sizeof(ull)
#define max(a,b) ({__typeof__(a) _a = (a);__typeof__(b) _b = (b);_a > _b? _a : _b;})
#define min(a,b) ({__typeof__(a) _a = (a);__typeof__(b) _b = (b);_a < _b? _a : _b;})

#endif 