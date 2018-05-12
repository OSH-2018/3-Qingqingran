#include <string.h>
#include <stdlib.h>
#include <errno.h>
#define FUSE_USE_VERSION 26
#include <fuse.h>
#include <sys/mman.h>
#include "fsxx.h"

typedef unsigned long long ull;

void * mem[BLOCK_NUM];

// 定义超级块结构体
typedef struct {
    ull block_total;
    ull block_occupy;
    ull block_remain;
} super_block_t;

// 定义文件属性结构体
typedef struct {
    char filename[256];
    struct stat st;
} filenode;

// 定义文件数据块结构体
typedef struct {
    char file_data[CONTENT_SIZE];
    ull chain_num;
} data_block_t;

// 标记块的使用情况,被占用标记为 1，未被占用标记为 0
void mark_block(ull i, int type){                                      
    ull pre_data; // 读取块中的原数据
    ull temp_data; // 应该写入的数据的位置
    ull ull_id = i / (8 * sizeof(ull));
    ull block_id = ull_id / (ULL_NUM) + 1;
    pre_data = ((ull *)mem[block_id])[ull_id % (ULL_NUM)];
    if (type == 1) { // 设置块被占用
        temp_data = 1ULL << (i % (8 * sizeof(ull)));
        ((ull *)mem[block_id])[ull_id % (ULL_NUM)] = pre_data | temp_data;
    }
    else if (type == 0){ // 设置块被释放
        temp_data = 1ULL << (i % (8 * sizeof(ull)));
        temp_data = ~temp_data;
        ((ull *)mem[block_id])[ull_id % (ULL_NUM)] = pre_data & temp_data;
    }
}

// 通过标记信息从头开始找空闲块
ull find_free(){
    ull i = 0;
    ull j = ~i;
    ull free_num;
    while((((ull *)mem[1 + (i / (8 * sizeof(ull))) / (ULL_NUM)])[(i / (8 * sizeof(ull))) % (ULL_NUM)]) == j)
        i += 8 * sizeof(ull);
    free_num = i + __builtin_ctzll(~(((ull *)mem[1 + (i / (8 * sizeof(ull))) / (ULL_NUM)])[(i / (8 * sizeof(ull))) % (ULL_NUM)]));
    return free_num;
}

void new_block(ull i){
    mem[i] = mmap(NULL, BLOCK_SIZE, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    memset(mem[i], 0, BLOCK_SIZE);
}

// 找到新的块并分配内存空间
ull create_block(){
    ull i = find_free();
    mem[i] = mmap(NULL, BLOCK_SIZE, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    memset(mem[i], 0, BLOCK_SIZE);
    mark_block(i, 1); // 将该分配的新块标记为已被占用
    // 更改文件系统的元数据信息
    ((super_block_t *)(mem[0]))->block_total = BLOCK_NUM;
    ((super_block_t *)(mem[0]))->block_occupy++;
    ((super_block_t *)(mem[0]))->block_remain--;
    return i;
}

// 文件目录增补
void index_add(ull num){
    ull i = 0;
    while((((ull *)mem[1 + BMBLOCK_NUM + i / (ULL_NUM)])[i % (ULL_NUM)]) != 0)
        i++;
    (((ull *)mem[1 + BMBLOCK_NUM + i / (ULL_NUM)])[i % (ULL_NUM)]) = num;
}

// 文件目录删减
void index_delete(ull num){
    ull i = 0;
    while((((ull *)mem[1 + BMBLOCK_NUM + i / (ULL_NUM)])[i % (ULL_NUM)]) != num)
        i++;
    ull j = i;
    while((((ull *)mem[1 + BMBLOCK_NUM + (i + 1) / (ULL_NUM)])[(i + 1) % (ULL_NUM)]) != 0)
        i++;
    ((ull *)mem[1 + BMBLOCK_NUM + j / (ULL_NUM)])[j % (ULL_NUM)] = ((ull *)mem[1 + BMBLOCK_NUM + i / (ULL_NUM)])[i % (ULL_NUM)];
    ((ull *)mem[1 + BMBLOCK_NUM + i / (ULL_NUM)])[i % (ULL_NUM)] = 0;
}

// 寻找开始读或写的块编号和位置
void search_wrblock(ull start_num, size_t offset, ull * dest_num, off_t * dest_place){
    ull len;
    len = (offset + CONTENT_SIZE - 1) / CONTENT_SIZE;
    *dest_num = start_num;
    // 返回值坐标指向待写或待读位置的前一个位置
    *dest_place = (offset + CONTENT_SIZE - 1) % CONTENT_SIZE;
    for (ull i = 0; i < len; i++){
        *dest_num = ((ull *)mem[*dest_num])[ULL_NUM - 1];  
    }    
}

// 删除某块之后关联的块
void destroy_block(ull start_num){    
    // 从该块开始逐个删除
    while(start_num != 0){
        ull temp_num = start_num;
        data_block_t *data_block = (data_block_t *)mem[start_num];
        start_num = data_block->chain_num;                
        munmap(mem[temp_num], BLOCK_SIZE);
        mark_block(temp_num, 0); // 标记该块的状态为未使用        
    }
}

// 初始化文件系统信息
void *fsxx_init(struct fuse_conn_info *conn){
    // 为元数据块、BM 块、index 块分配内存空间
    for(ull i = 0; i < 1 + BMBLOCK_NUM + IBLOCK_NUM; i++){ 
        new_block(i);
    }
    // 统计初始时内存使用情况
    ((super_block_t *)(mem[0]))->block_total = BLOCK_NUM;
    ((super_block_t *)(mem[0]))->block_occupy = 1 + BLOCK_NUM + IBLOCK_NUM;
    ((super_block_t *)(mem[0]))->block_remain = ((super_block_t *)(mem[0]))->block_total - ((super_block_t *)(mem[0]))->block_occupy;    
    // 标记初始时已用的块
    for(ull i = 0; i < 1 + BMBLOCK_NUM + IBLOCK_NUM; i++)
        mark_block(i,1);
}

// 创建一个新的文件
int fsxx_mknod(const char *path, mode_t mode, dev_t dev){
    // 搜寻空闲块并创建属性块
    ull num = create_block();  
    filenode *attr_block = (filenode *)mem[num];
    // 填补属性块的信息
    strcpy(attr_block->filename, path + 1);
    attr_block->st.st_mode = S_IFREG | 0644;
    attr_block->st.st_uid = fuse_get_context()->uid;
    attr_block->st.st_gid = fuse_get_context()->gid;
    attr_block->st.st_nlink = 1;
    attr_block->st.st_size = 0;
    // 将空闲块编号写入 index 块中
    index_add(num);
    return 0; 
}

// 寻找匹配的文件属性快
ull get_attr_block(const char *path, filenode * * attr_block){
    for(ull i = 1 + BMBLOCK_NUM; i < 1 + BMBLOCK_NUM + IBLOCK_NUM; i++)
        for(ull j = 0; j < ULL_NUM; j++){
            // 当所有文件匹配完毕未成功则返回 0
            if(((ull *)mem[i])[j] == 0)
                return 0ULL;
            ull attr_num = ((ull *)mem[i])[j];
            filenode *attr = (filenode *)mem[attr_num];
            // 文件匹配成功返回文件属性块编号
            if ((strcmp(attr->filename, path+1)) == 0){
                *attr_block = attr;
                return attr_num;
            }
        }    
}

// 得到文件属性
int fsxx_getattr(const char *path, struct stat *stbuf){
    if(strcmp(path, "/") == 0) {
        memset(stbuf, 0, sizeof(struct stat));
        stbuf->st_mode = S_IFDIR | 0755;
    } 
    else {
        filenode *attr_block;
        if (get_attr_block(path, &attr_block) != 0ULL){
            memcpy(stbuf, &(attr_block->st), sizeof(struct stat));
            return 0;
        }
        return -ENOENT;
    }
    return 0;
}

// 读文件属性
int fsxx_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi){
    filler(buf, ".", NULL, 0);
    filler(buf, "..", NULL, 0);
    for (ull i = 1 + BMBLOCK_NUM; i < 1 + BMBLOCK_NUM + IBLOCK_NUM; i++){
        int tag = 0;
        for (ull j = 0; j < ULL_NUM; j++){
            if (((ull *)mem[i])[j] != 0){
                ull num = ((ull *)mem[i])[j];
                filenode *attr = (filenode *)mem[num];
                filler(buf, attr->filename, &(attr->st), 0);
            }
            else {
                tag = 1;
                break;
            }    
        }
        if (tag == 1)
            break;
    }
    return 0;
}

// 打开一个文件
int fsxx_open(const char *path, struct fuse_file_info *fi){    
    return 0;
}

// 写文件函数
static int fsxx_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi){
    filenode *attr_block;
    size_t length, s = 0;
    ull attr_num;
    // 找到待写入文件的属性块
    if ((attr_num = get_attr_block(path, &attr_block)) == 0ULL)
        return -ENOENT;
    // 更改文件大小的属性
    attr_block->st.st_size = max(attr_block->st.st_size , (offset + size));
    ull wdest;
    off_t dest_place; 
    search_wrblock(attr_num, offset, &wdest, &dest_place);
    while (s < size){
        // 将 wdest 和 dest_place 移到待写入位置
        if (dest_place + 1 == CONTENT_SIZE){
            dest_place = 0;
            // 判读是否要分新的块
            if ((((data_block_t *)mem[wdest])->chain_num) == 0)
                ((data_block_t *)mem[wdest])->chain_num = create_block();
            wdest = ((data_block_t *)mem[wdest])->chain_num;                
        }             
        else
            dest_place++;       
        // 将内容写入
        length = min((size - s) , (CONTENT_SIZE - dest_place));
        memcpy(&((data_block_t *)mem[wdest])->file_data[dest_place], buf + s, length);
        dest_place = CONTENT_SIZE - 1;
        s += length;
    }
    return size;
}

// 截断文件
static int fsxx_truncate(const char *path, off_t size){
    filenode *attr_block; 
    ull attr_num;
    // 先找到文件对应的属性块
    if ((attr_num = get_attr_block(path , &attr_block)) == 0)
        return -ENOENT;
    // 若截断的位置在文件结束前
    if (attr_block->st.st_size > size){ 
        size_t s = 0; 
        ull wdest;
        off_t dest_place;      
        // 先找到开始截断的块中的某位置做特殊删除处理
        search_wrblock(attr_num, size, &wdest, &dest_place);
        ull temp_dest = wdest;
        if ((dest_place + 1) == CONTENT_SIZE){
            dest_place = 0;    
            wdest = ((data_block_t *)mem[temp_dest])->chain_num;
            ((data_block_t *)mem[temp_dest])->chain_num = 0;
        }
        else
            wdest = ((data_block_t *)mem[temp_dest])->chain_num;
        // 从开始截断的块的下一个块开始删除
        destroy_block(wdest);
        // 文件大小发生改变
        attr_block->st.st_size = size;
    }
    // 若截断的位置在文件结束后
    else{
        char *buf = (char *) malloc(sizeof(char) * (size - attr_block->st.st_size));
        memset(buf, 0, (size - attr_block->st.st_size));
        fsxx_write(path, buf, (size - attr_block->st.st_size), attr_block->st.st_size, NULL);
    }
    return 0;
}

static int fsxx_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi){
    filenode *attr_block;
    ull attr_num;
    size_t length, file_length, s = 0;
    // 找到文件的属性块
    if ((attr_num = get_attr_block(path, &attr_block)) == 0ULL)
        return -ENOENT;       
    ull wdest;
    off_t dest_place;
    search_wrblock(attr_num, offset, &wdest, &dest_place);
    while (s < size){
        // 将 wdest 和 dst_place 移到待读的位置
        if (dest_place + 1 == CONTENT_SIZE){
            dest_place = 0;
            wdest = ((data_block_t *)mem[wdest])->chain_num;
            if (wdest == 0)
                return s;
        }      
        else 
            dest_place++;            
        // 若读不到文件尾部，则正常读出
        if (((data_block_t *)mem[wdest])->chain_num != 0){
            length = min((size - s) , (CONTENT_SIZE - dest_place));
            memcpy(buf + s, &((data_block_t *)mem[wdest])->file_data[dest_place], length);
            dest_place = CONTENT_SIZE - 1;
            s += length;
        }
        // 若要读到文件尾部
        else{
            length = min((size - s) , (attr_block->st.st_size - s));
            memcpy(buf + s, &((data_block_t *)mem[wdest])->file_data[0], length);
            s += length;
            return s;
        }            
    }
    return s;    
}

// 删除文件
static int fsxx_unlink(const char *path){
    ull attr_num;
    filenode *attr_block;
    if((attr_num = get_attr_block(path, &attr_block)) == 0)
        return -ENOENT;
    index_delete(attr_num); // 删除该文件的目录项
    destroy_block(attr_num); // 删除该文件的所有块
    return 0;
}

static const struct fuse_operations op = {
    .init = fsxx_init,
    .getattr = fsxx_getattr,
    .readdir = fsxx_readdir,
    .mknod = fsxx_mknod,
    .open = fsxx_open,
    .write = fsxx_write,
    .truncate = fsxx_truncate,
    .read = fsxx_read,
    .unlink = fsxx_unlink,
};

int main(int argc, char *argv[]){
    return fuse_main(argc, argv, &op, NULL);
}