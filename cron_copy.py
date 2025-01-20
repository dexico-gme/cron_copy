import os, time, subprocess
import glob
from math import exp, expm1
import logging
import queue
import asyncio
import configparser
import csv

config = configparser.ConfigParser()
config.read('config/config.ini')

''''
This script will look for incoming folders in a path and then compare against the existing folder list of a path. 
If it is a new folder then it will add that folder to a queue and start to compare the folder size every 10 seconds. 
If the data is still incrementing then it will keep on watching the folder until the filesize stops going up. 
Once the data has finished transferring then it will copy that data to a vendor folder. 
The script will retry a copy if it doesn't return a 0 subprocess code for rsync. 

Temporary files to look out for that can cause non-existant path errors are .ext & .mrv
'''
log_path = config['Paths']['log_path']
q = queue.Queue()
logger = logging.getLogger(__name__)
home_dir = os.path.expanduser('~')
logging.root.handlers = []
logging.basicConfig(level=logging.DEBUG, 
        format='%(asctime)s: %(levelname)s: %(message)s',
        handlers=[
        logging.FileHandler(f'{home_dir}{log_path}'),
        logging.StreamHandler()] )

'''paths stored in config.py, change path variables for project'''
raid_path = config['Paths']['raid_path']
vfx_srv_path = config['Paths']['vfx_srv_path']
csv_path = config['Paths']['csv_path']
async def size_check(input_path, output_path):
    input_size = 0
    output_size = 0
    #write csv
    try:
        if os.path.isfile(csv_path):
            pass
            
    except:
        with open(f'{csv_path}', 'w', newline='') as csvfile:
            fieldnames = ['input folder','input size', 'output_folder' 'output size']
            spamwriter = csv.writer(csvfile, delimiter=' ', quotechar='|', quoting=csv.QUOTE_MINIMAL)
            spamwriter.writerow(fieldnames)

    for path in (sorted(glob.glob(f'{input_path}'))):
                input_size = 0
                for p, dirs, files in os.walk(path):
                    for f in files:
                        if 'exr-sv.tmp' in f:
                            pass
                        elif '.exr-sv.met' in f:
                            pass
                        else:
                            fp = os.path.join(p, f)
                            input_size += os.path.getsize(fp)
                # raid_pkg_path_list.append(path)
                # raid_pkg_name_list.append(os.path.basename(path))
                logger.info(f'input_path: {input_path}')
                logger.info(f'input path: {round((input_size/1024/1024/1024),2)}')

    '''output path size check'''
    for path in (sorted(glob.glob(f'{output_path}'))):
                output_size = 0
                for p, dirs, files in os.walk(path):
                    for f in files:
                        if 'exr-sv.tmp' in f:
                            pass
                        elif '.exr-sv.met' in f:
                            pass
                        else:
                            fp = os.path.join(p, f)
                            output_size += os.path.getsize(fp)
                # raid_pkg_path_list.append(path)
                # raid_pkg_name_list.append(os.path.basename(path))
                logger.info(f'input_path: {output_path}')
                logger.info(f'output path: {round((output_size/1024/1024/1024),2)}')
    
    with open(f'{csv_path}', 'a', newline='') as csvfile:
        fieldnames = ['input folder','input size', 'output_folder' 'output size']
        spamwriter = csv.writer(csvfile, delimiter=' ', quotechar='|', quoting=csv.QUOTE_MINIMAL)
        spamwriter.writerow([input_path, input_size, output_path, output_size])


async def until_ready(q):
    """compare file sizes whilst the new package lands, once the filesize stops increasing then copy the package to the raid"""
    path = q
    print(f'[UNTIL READY]: {path}')
    size_1 = 0
    size_2 = 0
    filesize_1 = 0
    filesize_2 = 1
    
    while True:
        try:
            while filesize_1 != filesize_2:
                size_1 = 0
                for p, dirs, files in os.walk(path):
                    for f in files:
                        if 'exr-sv.tmp' in f:
                            pass
                        elif '.exr-sv.met' in f:
                            pass
                        else:
                            fp = os.path.join(p, f)
                            size_1 += os.path.getsize(fp)
                filesize_1 = round((size_1/1024/1024/1024),8)
                logger.info(f'[filesize_1]: {filesize_1}')
                logger.info('sleeping 10s')
                time.sleep(10)
                logger.info('awake')
                size_2 = 0
                for p_2, dirs_2, files_2 in os.walk(path):
                    for f_2 in files_2:
                        if 'exr-sv.tmp' in f_2:
                            pass
                        elif '.exr-sv.met' in f_2:
                            pass
                        else:
                            fp_2 = os.path.join(p_2, f_2)
                            size_2 += os.path.getsize(fp_2)
                filesize_2 = round((size_2/1024/1024/1024),8)
                logger.info(f'[filsize_2]: {filesize_2}')
        except FileNotFoundError:
            logger.warning('FileNotFoundError, going to sleep s & try again')
            time.sleep(30)
            logger.info('awake')
            while filesize_1 != filesize_2:
                size_1 = 0
                for p, dirs, files in os.walk(path):
                    for f in files:
                        if 'exr-sv.tmp' in f:
                            pass
                        elif '.exr-sv.met' in f:
                            pass
                        else:
                            fp = os.path.join(p, f)
                            size_1 += os.path.getsize(fp)
                filesize_1 = round((size_1/1024/1024/1024),8)
                logger.info(f'[filesize_1]: {filesize_1}')
                logger.info('sleeping 10s')
                time.sleep(10)
                logger.info('awake')
                size_2 = 0
                for p_2, dirs_2, files_2 in os.walk(path):
                    for f_2 in files_2:
                        if 'exr-sv.tmp' in f_2:
                            pass
                        elif '.exr-sv.met' in f_2:
                            pass
                        else:
                            fp_2 = os.path.join(p_2, f_2)
                            size_2 += os.path.getsize(fp_2)
                filesize_2 = round((size_2/1024/1024/1024),8)
                logger.info(f'[filsize_2]: {filesize_2}')

        else: 
            if filesize_1 == filesize_2:
                p_split = path.split("/")
                package = p_split[-1]
                vendor = p_split[-2]
                logger.info('[VENDOR]: {vendor}')
                full_path = f'{raid_path}{vendor}'
                logger.info(f'[FULL PATH]: {full_path}')
                if not os.path.exists(f'{full_path}/{package}'): 
                    os.makedirs(f'{full_path}/{package}')
                    p = subprocess.run(["rsync", "-avhWP", f'{path}/{package}', full_path])
                    while p.returncode != 0:
                        logger.warning('[ERROR RE-RUNNING RSYNC AFTER 10s]')
                        time.sleep(10)
                        logger.info('awake')
                        p = subprocess.run(["rsync", "-avhWP", f'{path}/{package}', full_path])
                    await size_check(f'{path}/{package}',f'{full_path}/{package}')
                    break
                else:
                    logger.info('[PATH EXISTS]')
                    p = subprocess.run(["rsync", "-avhWP", f'{path}/{package}', full_path])
                    while p.returncode != 0:
                        logger.warning('[ERROR RE-RUNNING RSYNC AFTER 10s]')
                        time.sleep(10)
                        logger.info('awake')
                        p = subprocess.run(["rsync", "-avhWP", f'{path}/{package}', full_path])
                    await size_check(f'{path}/{package}',f'{full_path}/{package}')
                    break
            else:
                 break
async def main():
    while True:

        raid_pkg_path_list = []
        raid_pkg_name_list = []

        vfx_srv_path_list = []
        vfx_srv_pkg_list = []

        for path in (sorted(glob.glob(f'{raid_path}/*/*'))):
            """filesize check omit"""
            # size = 0
            # for p, dirs, files in os.walk(path):
            #     for f in files:
            #         if 'exr-sv.tmp' in f:
            #             pass
            #         else:
            #             fp = os.path.join(p, f)
            #             size += os.path.getsize(fp)
            raid_pkg_path_list.append(path)
            raid_pkg_name_list.append(os.path.basename(path))
            #logger.info(round((size/1024/1024/1024),2))

        for path in (sorted(glob.glob(f'{vfx_srv_path}*/*'))):
            """filesize check omit"""
            # size = 0
            # for p, dirs, files in os.walk(path):
            #     for f in files:
            #         if 'exr-sv.tmp' in f:
            #             pass
            #         fp = os.path.join(p, f)
            #         size += os.path.getsize(fp)
            vfx_srv_path_list.append(path)
            vfx_srv_pkg_list.append(os.path.basename(path))
            #logger.info(round((size/1024/1024/1024),2))

        diff_list = [path for path in vfx_srv_pkg_list if path not in raid_pkg_name_list]

        for item in diff_list:
            print(item)
            for path in vfx_srv_path_list:
                print(path)
                if item in os.path.basename(path):
                    print(item)
                    print(f'[Q.PUT] {q.put(path)}')

        logger.info(f'[diff_list]: {diff_list}')
        logger.info(f'[raid_pkg_name_list]: \n\n{raid_pkg_name_list}\n\n')
        logger.info(f'[vfx_srv_pkg_list]: \n\n{vfx_srv_pkg_list}\n\n')
        logger.info(f'[raid_pkg_list]: \n\n{raid_pkg_path_list}\n\n')
        logger.info(f'[vfx_srv_path_list]: \n\n{vfx_srv_path_list}\n\n')

        # check folder paths against dirs list
        # if new then add to queue_list & append to dirs_list
        # once in dirs list check file size every 2 mins. 
        # if the file size increases then wait, else copy the folder
        # once copy is complete then remove from queue. 
        # sleep for 5 minutes

        queue_list = []

        logger.info(f'[QUEUE SIZE] {q.qsize()}')
        
        if q.qsize() == 0:
            print('sleeping for 2m')
            await asyncio.sleep(120)
        else:
            await until_ready(q.get())





if __name__ == "__main__":
    asyncio.run(main())
        
        