import os
import shutil
import threading
import time

from loguru import logger


class FileManage:
    def __init__(self):
        self.root_path = os.path.dirname(__file__)
        self.dirPath = str(self.root_path[0:-5]) + "/static/csv"
        logger.remove()
        logger.add(str(self.root_path[0:-5]) + "/logs/log.log")
        self.logger = logger

    def autoDelete(self, tm_hour, tm_min):
        localtime = time.localtime(time.time())
        while True:
            # print(localtime)
            if localtime.tm_hour == int(tm_hour) and localtime.tm_min == int(tm_min):
                for root, dirs, files in os.walk(self.dirPath, topdown=False):
                    for name in files:
                        os.remove(os.path.join(root, name))
                        print("%s文件删除成功 %s" % (name, (time.strftime("%d/%m/%Y%H:%M:%S"))))
                    for name in dirs:
                        os.rmdir(os.path.join(root, name))
                        print("%s子文件夹下文件删除成功 %s" % (name, (time.strftime("%d/%m/%Y%H:%M:%S"))))

    def deleteFolder(self, tm_hour, tm_min):
        localtime = time.localtime(time.time())
        try:
            if localtime.tm_hour == int(tm_hour) and localtime.tm_min == int(tm_min):
                shutil.rmtree(self.dirPath)
                os.mkdir(self.dirPath)
                self.logger.info(str(self.logger) + "文件夹内容删除成功")
        except Exception as e:
            self.logger.info("文件夹删除失败:" + str(e))


class myThread(threading.Thread):
    def __init__(self, threadID, name, delay, tm_hour, tm_min):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.delay = delay
        self.tm_hour = tm_hour
        self.tm_min = tm_min

    def run(self):
        print("开始线程：" + self.name)
        FileManage().deleteFolder(self.tm_hour, self.tm_min)
        print("退出线程：" + self.name)

# if __name__ == '__main__':
#     # 创建新线程
#     autoDeleteThread = myThread(1, "Thread-1", 1, 14, 38)
#     autoDeleteThread.start()
#     autoDeleteThread.join()
#     FileManage().deleteFolder(14,36)
