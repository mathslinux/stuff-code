#include <rbd/librbd.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>

static rados_t cluster;
static rados_ioctx_t io_ctx;
static char *poolname;
static rbd_image_t image;
static char* imagename;
static char *monitor;
static char *username;
static char *password;

static int done;

static void app_finish_aiocb(rbd_completion_t c, char *buf)
{
    int ret;
    ret = rbd_aio_get_return_value(c);
    if (ret < 0) {
        fprintf(stderr, "error reading image:%s, %s", imagename,
                strerror(-ret));
    }
    fprintf(stdout,
            "buffer read:\n"
            "========================================\n"
            "%s\n"
            "========================================\n",
            buf);
    rbd_aio_release(c);
    done = 1;
}

static void app_run()
{
    char *buf;
    int ret;
	rados_completion_t comp;

    buf = (char *)malloc(128);
    /**
     * rados 支持异步操作, 当执行大量 I/O 的时候, 不需要等待每一个操作完成,
     * 只需要传递一个回调函数给读写函数, 这样, 操作完成, librados 会自动条用
     * 我们的回调函数.
     */
	ret = rbd_aio_create_completion(buf, (rbd_callback_t)app_finish_aiocb, &comp);
	if (ret < 0) {
		fprintf(stderr, "could not create aio completion: %s\n",
                strerror(-ret));
        return;
    }

    /**
     * 异步读的 API: rbd_aio_read(), 分别传入 image 的句柄, 读的开始位置
     * 读多少字节, 存储的指针, 最后是上面的异步句柄
     */
	ret = rbd_aio_read(image, 0, 128, buf, comp);
    if (ret < 0) {
        fprintf(stderr, "error reading image:%s, %s\n", imagename, strerror(-ret));
        return;
    }
    for (; ;) {
        if (done) {
            break;
        }
        /* Waiting for the reading operation to finish */
        usleep(100000);
    }
}

static int conf_set(const char *key, const char *value)
{
    int r;
    r = rados_conf_set(cluster, key, value);
    if (r < 0) {
		fprintf(stderr, "invalid conf option: %s\n", key);
    }
    return r;
}

static int app_rbd_open()
{
	int ret;
    /* poolname imagename username password monitor */
    fprintf(stdout,
            "open rados as following setting:\n"
            "poolname: %s\n"
            "imagename: %s\n"
            "username: %s\n"
            "password: %s\n"
            "monitor: %s\n",
            poolname, imagename, username, password, monitor);

    /**
     * 创建一个 rados_t 句柄, 该句柄暴扣了rados 客户端的数据结构, 用来和
     * rados 通信, 第二个参数是连接 rados 的客户端 ID, 这里我们使用 admin
     */
	ret = rados_create(&cluster, username);
	if (ret < 0) {
		fprintf(stderr, "cannot create a cluster handle: %s\n", strerror(-ret));
        goto failed_create;
	}

    /* 载入配置文件 */
	ret = rados_conf_read_file(cluster, "/etc/ceph/ceph.conf");
	if (ret < 0) {
		fprintf(stderr, "cannot read config file: %s\n", strerror(-ret));
        goto failed_shutdown;
	}

    /**
     * 调用 rados_conf_set() 设置 rados 参数, 包括认证信息, monitor 地址等,
     * 如果设置了 cephx 认证, 那么之前创建 rados 句柄的时候, 必须设置客户端
     * ID, 并且必须设置 key 的密码.
     */
	if (conf_set("key", password) < 0) {
        goto failed_shutdown;
    }
	if (conf_set("auth_supported", "cephx") < 0) {
        goto failed_shutdown;
    }
	if (conf_set("mon_host", monitor) < 0) {
        goto failed_shutdown;
    }

    /* 完成了上面的设置之后, 就可以用我们的 rados 句柄连接 rados 服务器了 */
	ret = rados_connect(cluster);
	if (ret < 0) {
		fprintf(stderr, "cannot connect to cluster: %s\n", strerror(-ret));
        goto failed_shutdown;
	}

    /**
     * 成功连接上 rados 服务器之后, 就可以用 rados_ioctx_create() 打开 rados
     * 上的 pool 了, 该函数需要传递一个 rados_ioctx_t, 用来对打开的 pool 进行
     * 操作.
     */
	ret = rados_ioctx_create(cluster, poolname, &io_ctx);
	if (ret < 0) {
		fprintf(stderr, "cannot open rados pool %s: %s\n",
                poolname, strerror(-ret));
        goto failed_shutdown;
	}

    /**
     * 上面说过, 我们已经能够有一个 rados_ioctx_t 的指针了, 该指针用来对关联的
     * pool 进行 I/O 操作, 比如打开池中的 image 等, 这里我们直接对 rbd 操作. 
     */
	ret = rbd_open(io_ctx, imagename, &image, NULL);
	if (ret < 0) {
		fprintf(stderr, "error reading header from image %s\n", imagename);
        goto failed_open;
	}
    return 0;

failed_open:
    rados_ioctx_destroy(io_ctx);
failed_shutdown:
    rados_shutdown(cluster);
failed_create:
    return ret;
}

static void app_rbd_close()
{
    rados_ioctx_destroy(io_ctx);
    rados_shutdown(cluster);
}

int main(int argc, char **argv)
{
    if (argc != 6) {
        fprintf(stderr,
                "usage: %s poolname imagename username password monitor\n",
                argv[0]);
        return -1;
    }

    poolname = argv[1];
    imagename = argv[2];
    username = argv[3];
    password = argv[4];
    monitor = argv[5];

    if (app_rbd_open() == 0) {
        app_run();
        app_rbd_close();        
    }

    return 0;
}
