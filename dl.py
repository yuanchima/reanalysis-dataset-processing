import time
import os
import concurrent.futures

def download(user_name, password):
    def dl(input_file):
        cmd = f'wget -N -nv --load-cookies .urs_cookies --save-cookies .urs_cookies --auth-no-challenge=on --keep-session-cookies --user={user_name} --password={password} --content-disposition -i {input_file}'
        os.system(cmd)
    return dl

def load_url(file_path):
    with open(file_path, 'r') as f:
        all_url = f.readlines()
    all_url = [url.replace("\n", "") for url in all_url]
    return all_url

def split_file(all_url, file_nums, subset_root='./subset'):
    if os.path.exists(subset_root):
        files = os.listdir(subset_root)
        for f in files:
            os.remove(f"{subset_root}/{f}")
    else:
        os.mkdir(subset_root)

    rows_per_file = len(all_url) // file_nums

    for i in range(file_nums):
        with open(f'{subset_root}/subset_{i}.txt', 'w') as f:
            if i != rows_per_file-1:
                f.writelines([f'{url}\n' for url in all_url[i*rows_per_file: (i+1)*rows_per_file]])
            else:
                f.writelines([f'{url}\n' for url in all_url[i*rows_per_file:]])
        
def threadPoolExecutor(file_list, downloader, num_worker=5):
    with concurrent.futures.ThreadPoolExecutor(num_worker) as executor:
        results = [executor.submit(downloader, f) for f in file_list]
        for f in concurrent.futures.as_completed(results):
            print(f.result())


if __name__ == "__main__":
    user_name = input("user name: ")
    password = input("password: ")
    downloader = download(user_name, password)

    start = time.perf_counter()

    file_path = 'subset_M2T1NXSLV_5.12.4_20210104_062202.txt'
    all_url = load_url(file_path)
    subset_root = './subset'
    split_file(all_url, 100, subset_root)
    f_lst= [f'{subset_root}/{f}' for f in os.listdir(subset_root)]

    threadPoolExecutor(f_lst, downloader, num_worker=50)
    # download(f_lst[0])

    finish = time.perf_counter()
    print(f'Finished in {round(finish-start, 2)} second(s)')