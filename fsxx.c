#include <string.h>
#include <stdlib.h>
#include <errno.h>
#define FUSE_USE_VERSION 26
#include <fuse.h>
#include <sys/mman.h>
#include "fs.h"

typedef unsigned long long ull;

void * mem[BLOCK_NUM];

// 超级块结构体定义
typedef struct {
    ull block_total;
    ull block_occupy;
    ull block_remain;
} super_block_t;

// 文件属性结构体
typedef struct {
    char filename[256];
    struct stat st;
} filenode;

// 标记块的使用情况,被占用标记为 1，未被占用标记为 0
void mark_block(ull i, int type){                                      
    ull pre_data; // 读取块中的原数据
    ull temp_data; // 应该写入的数据的位置
    pre_data = ((ull *)mem[1 + i * sizeof(ull)/BLOCK_SIZE])[i % (BLOCK_SIZE / sizeof(ull))];
    if (type == 1) { // 设置块被占用
        temp_data = 1ULL << (i % (8 * sizeof(ull)));
        ((ull *)mem[1 + (i / (8 * sizeof(ull))) / (BLOCK_SIZE / sizeof(ull))])[(i / (8 * sizeof(ull))) % (BLOCK_SIZE / sizeof(ull))] = pre_data | temp_data;
    }
    else if (type == 0){ // 设置块被释放
        temp_data = 1ULL << (i % (8 * sizeof(ull)));
        temp_data = ~temp_data;
        ((ull *)mem[1 + (i / (8 * sizeof(ull))) / (BLOCK_SIZE / sizeof(ull))])[(i / (8 * sizeof(ull))) % (BLOCK_SIZE / sizeof(ull))] = pre_data & temp_data;
    }
}

// 通过标记信息从头开始找空闲块
ull find_free(){
    ull i = 0;
    ull j = ~i;
    ull free_num;
    while((((ull *)mem[(i / (8 * sizeof(ull))) / (BLOCK_SIZE / sizeof(ull))])[(i / (8 * sizeof(ull))) % (BLOCK_SIZE / sizeof(ull))] | 0ULL) == j)
        i += 8 * sizeof(ull);
    free_num = 8 * sizeof(ull) * i + __builtin_ctzll(~(((ull *)mem[(i / (8 * sizeof(ull))) / (BLOCK_SIZE / sizeof(ull))])[(i / (8 * sizeof(ull))) % (BLOCK_SIZE / sizeof(ull))]));
    return free_num;
}

// 文件目录增补
void index_add(ull num){
    ull i = 0;
    while((((ull *)mem[1 + BMBLOCK_NUM + i / (BLOCK_SIZE / sizeof(ull))])[i % (BLOCK_SIZE / sizeof(ull))]) != 0)
        i++;
    (((ull *)mem[1 + BMBLOCK_NUM + i / (BLOCK_SIZE / sizeof(ull))])[i % (BLOCK_SIZE / sizeof(ull))]) = num;
}

// 文件目录删减
void index_delete(ull num){
    ull i = 0;
    while((((ull *)mem[1 + BMBLOCK_NUM + i / (BLOCK_SIZE / sizeof(ull))])[i % (BLOCK_SIZE / sizeof(ull))]) != num)
        i++;
    ull j = i;
    while((((ull *)mem[1 + BMBLOCK_NUM + i / (BLOCK_SIZE / sizeof(ull))])[i % (BLOCK_SIZE / sizeof(ull))]) != 0)
        i++;
    ((ull *)mem[1 + BMBLOCK_NUM + j / (BLOCK_SIZE / sizeof(ull))])[j % (BLOCK_SIZE / sizeof(ull))] = ((ull *)mem[1 + BMBLOCK_NUM + i / (BLOCK_SIZE / sizeof(ull))])[i % (BLOCK_SIZE / sizeof(ull))];
    ((ull *)mem[1 + BMBLOCK_NUM + i / (BLOCK_SIZE / sizeof(ull))])[i % (BLOCK_SIZE / sizeof(ull))] = 0;
}

// 寻找匹配的文件属性快
ull get_attr_block(const char *path, filenode *attr_block){
    for(ull i = 1 + BMBLOCK_NUM; i < 1 + BMBLOCK_NUM + IBLOCK_NUM; i++)
        for(ull j = 0; j < BLOCK_SIZE / sizeof(ull); j++){
            if(((ull *)mem[i])[j] != 0){
                ull attr_num = ((ull *)mem[i])[j];
                filenode *attr = (filenode *)mem[attr_num];
                if ((strcmp(attr->filename, path+1)) == 0){
                    attr_block = attr;
                    return attr_num;
                }
            }
            else if (((ull *)mem[i])[j] == 0)
                return 0ULL;
        }      
}

void *fsxx_init(struct fuse_conn_info *conn){
    // 为元数据块、BM 块、index 块分配内存空间
    for(ull i = 0; i < 1 + BMBLOCK_NUM + IBLOCK_NUM; i++){ 
        mem[i] = mmap(NULL, BLOCK_SIZE, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        memset(mem[i], 0, BLOCK_SIZE);
    }
    // 统计初始时内存使用情况
    ((super_block_t *)(mem[0]))->block_total = BLOCK_NUM;
    ((super_block_t *)(mem[0]))->block_occupy = 1 + BLOCK_NUM + IBLOCK_NUM;
    ((super_block_t *)(mem[0]))->block_remain = ((super_block_t *)(mem[0]))->block_total - ((super_block_t *)(mem[0]))->block_occupy;    
    // 标记初始时已用的块
    for(ull i = 0; i < 1 + BMBLOCK_NUM + IBLOCK_NUM; i++)
        mark_block(i,1);
}

int fsxx_mknod(const char *path, mode_t mode, dev_t dev){
    // 搜寻空闲块
    ull num = find_free();   
    filenode *attr_block = (filenode *)mem[num];
    strcpy(attr_block->filename, path + 1);
    attr_block->st.st_mode = S_IFREG | 0644;  // 文件类型和存取权限
    attr_block->st.st_uid = fuse_get_context()->uid;
    attr_block->st.st_gid = fuse_get_context()->gid;
    attr_block->st.st_nlink = 1;
    attr_block->st.st_size = 0;
    // 将该块的状态标记为已占用
    mark_block(num, 1);
    // 将空闲块编号写入 index 块中
    index_add(num);
    // 更改文件系统元信息
    ((super_block_t*)(mem[0]))->block_occupy++;
    ((super_block_t*)(mem[0]))->block_remain--;
    return 0; 
}

int fsxx_getattr(const char *path, struct stat *stbuf){
    if(strcmp(path, "/") == 0) {
        memset(stbuf, 0, sizeof(struct stat));
        stbuf->st_mode = S_IFDIR | 0755;
    } 
    else {
        filenode *attr_block;
        if (get_attr_block(path, attr_block) != 0ULL){
            memcpy(stbuf, &(attr_block->st), sizeof(struct stat));
            return 0;
        }
        return -ENOENT;
    }
    return 0;
}

int fsxx_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi){
    filler(buf, ".", NULL, 0);
    filler(buf, "..", NULL, 0);
    for (ull i = 1 + BMBLOCK_NUM; i < 1 + BMBLOCK_NUM + IBLOCK_NUM; i++){
        int tag = 0;
        for (ull j = 0; j < BLOCK_SIZE / sizeof(ull); j++){
            if(((ull *)mem[i])[j] != 0){
                ull num = ((ull *)mem[i])[j];
                filenode *attr = (filenode *)mem[num];
                filler(buf, attr->filename, &(attr->st), 0);
            }   
            tag = 1;
            break;
        }
        if (tag == 1)
            break;
    }
    return 0;
}

int fsxx_open(const char *path, struct fuse_file_info *fi){    
    return 0;
}

static int fsxx_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi){
    filenode *attr_block;
    ull attr_num;
    if ((attr_num = get_attr_block(path, attr_block)) != 0ULL){
        attr_block->st.st_size = offset + size;
        size_t s;
        s = 0;
        ull wdest = ((ull *)mem[attr_num])[BLOCK_SIZE / sizeof(ull) - 1];
        ull temp_dest;
        while (s < size){
            ull length = (size - s) < CONTENT_SIZE? (size - s) : CONTENT_SIZE;
            memcpy(mem[wdest], buf + s, length);
            mark_block(wdest, 1);
            temp_dest = wdest;
            if ((wdest = find_free()) == 0)
                return -ENOENT;
            ((ull *)mem[temp_dest])[BLOCK_SIZE / sizeof(ull) - 1] = wdest;
            s += length;
        }
        ((ull *)mem[temp_dest])[BLOCK_SIZE / sizeof(ull) - 1] = 0ULL;
        return size;
    }
    else if ((attr_num = get_attr_block(path, attr_block)) == 0ULL)
        return -ENOENT;
}

/*static int oshfs_truncate(const char *path, off_t size)
{
    filenode *node = get_filenode(path);
    node->st.st_size = size;
    node->content = realloc(node->content, size);
    return 0;
}*/

/*static int fsxx_truncate(const char *path, off_t size){
    filenode *attr_block;
    ull attr_num = get_attr_block(path, attr_block);
    
}*/

static int fsxx_unlink(const char *path)
{
    filenode *attr_block;
    ull attr_num;

    if ((attr_num = get_attr_block(path, attr_block)) != 0){
        while(attr_num != 0ULL){
            ull next_block = ((ull *)mem[attr_num])[BLOCK_SIZE / sizeof(ull) -1];
            munmap(mem[attr_num], BLOCK_SIZE);
            index_delete(attr_num);
            mark_block(attr_num, 0);
            attr_num = next_block;
        } 
        return 0;      
    }
    else if ((attr_num = get_attr_block(path, attr_block)) == 0)
        return -ENOENT;
}

static int fsxx_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi){
    filenode *attr_block;
    ull attr_num;
    if ((attr_num = get_attr_block(path, attr_block)) != 0ULL){
        attr_block->st.st_size = offset + size;
        size_t s;
        s = 0;
        ull wdest = ((ull *)mem[attr_num])[BLOCK_SIZE / sizeof(ull) - 1];
        while (s < size){
            ull length = (size - s) < CONTENT_SIZE? (size - s) : CONTENT_SIZE;
            memcpy(buf + s, mem[wdest], length);
            wdest = ((ull *)mem[wdest])[BLOCK_SIZE / sizeof(ull) - 1];
            s += length;
        }
        return s;
    }
    return -ENOENT;
}

static const struct fuse_operations op = {
    .init = fsxx_init,
    .getattr = fsxx_getattr,
    .readdir = fsxx_readdir,
    .mknod = fsxx_mknod,
    .open = fsxx_open,
    .write = fsxx_write,
    // .truncate = fsxx_truncate,
    .read = fsxx_read,
    .unlink = fsxx_unlink,
};

int main(int argc, char *argv[]){
    return fuse_main(argc, argv, &op, NULL);
}