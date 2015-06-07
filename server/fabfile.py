# coding: utf-8
from fabric.api import run, env
from fabric.exceptions import NetworkError
import time, random
source_str = 'abcdefghijklmnopqrstuvwxyz'
env.user = 'root'
env.key_filename = '/home/id_rsa'


def get_lxc_name(ip, is_master):
    """
    ipアドレスの下位二桁をmaster-/slave-の後に繋げたものをLXCコンテナの名前として返す.
    """
    return ('master-' if is_master else 'slave-') + ip[-2:]


def set_host_string(ip_or_lxc_name):
    """
    ipアドレスまたはコンテナ名に応じてfabric.api.env.host_stringを再設定.
    """
    env.host_string = ("157.82.3.140" if int(ip_or_lxc_name[-1]) % 2 == 0 else "157.82.3.141")


def remote_run(ip_or_lxc_name, command):
    """
    ipアドレスに応じたホスト上でcommandを実行する.
    """
    set_host_string(ip_or_lxc_name)
    run(command)


def make_lxc(ip, is_master):
    """
    LXCコンテナを生成し、/etc/interfacesを編集することでipアドレスを設定する.
    """
    lxc_name = get_lxc_name(ip, is_master)
    remote_run(ip, 'lxc-clone -o template -n ' + lxc_name)
    
    lxc_root            = '/var/lib/lxc/' + get_lxc_name(ip, is_master) + '/rootfs'
    interfaces_file     = lxc_root + '/etc/network/interfaces'
    interfaces_templace = lxc_root + '/etc/network/interfaces.template'

    command = 'cat {0} | sed s/IP_ADDRESS/{1}/g > {2}' \
      .format(interfaces_templace, ip, interfaces_file)
    remote_run(ip, command)


def create_cluster(master, *slave_list):

    def get_all_lxc_names():
        lxc_names = [get_lxc_name(master, True)]
        for slave in slave_list:
            lxc_names.append(get_lxc_name(slave, False))    
        return lxc_names
    
    def get_name_resolutions():
        """
        ホスト名とIPアドレスの対応関係を生成.
        """
        name_resolutions = []
        name_resolutions.append(master + ' g4s' + master[-1])
        name_resolutions.append(master + ' ' + get_lxc_name(master, True))
        name_resolutions.append(master + ' master')
        for slave in slave_list:
            name_resolutions.append(slave + ' ' + get_lxc_name(slave, False))
        return name_resolutions

    def configure_hosts(lxc_name):
        """
        各コンテナの/etc/hostsを編集しホスト名を解決する.
        /etc/hosts.template内のNAME_RESOLUTIONを、必要とする名前の対応に入れ替える.
        """
        lxc_root       = '/var/lib/lxc/' + lxc_name + '/rootfs/'
        hosts_template = lxc_root + 'etc/hosts.template'
        hosts_file     = lxc_root + 'etc/hosts'
        
        command = 'cat {0} | sed s/NAME_RESOLUTION/"{1}"/g > {2}' \
          .format(hosts_template, '\\n'.join(get_name_resolutions()), hosts_file)
        remote_run(lxc_name, command)

    def configure_slaves():
        """
        masterにslaveのIPアドレスを教える
        """
        slave_file = '/var/lib/lxc/' + get_lxc_name(master, True) +  \
          '/rootfs/home/hadoop/hadoop-2.6.0/etc/hadoop/slaves'
        command = 'echo "{0}" > {1}'.format('\n'.join(get_all_lxc_names()), slave_file)
        remote_run(master, command)

    # 1. 各コンテナを生成しIPアドレスを割り振る
    make_lxc(master, is_master=True)
    for slave in slave_list:
        make_lxc(slave, is_master=False)

    # 2 各コンテナのhostsを設定
    for lxc_name in get_all_lxc_names():
        configure_hosts(lxc_name)
    
    # 3. マスターのslavesをいじる
    configure_slaves()
    
    # 4. コンテナを起動する
    for lxc_name in get_all_lxc_names():
        remote_run(lxc_name, 'lxc-start -n ' + lxc_name + ' -d')

    # 5. Hadoopクラスタを起動
    env.host_string = master
    env.user        = 'hadoop'
    while True:
        try:
            run('hdfs namenode -format')
        except NetworkError:
            time.sleep(1)
        else:
            break
    run('start-dfs.sh')
    run('start-yarn.sh')


def destroy_cluster(master, *slave_list):
    master_name = get_lxc_name(master, is_master=True)
    remote_run(master, "lxc-stop -n " + master_name)
    remote_run(master, "lxc-destroy -n " + master_name)
    
    for slave_ip in slave_list:
        slave_name = get_lxc_name(slave_ip, is_master=False)
        remote_run(slave_ip, "lxc-stop -n " + slave_name)
        remote_run(slave_ip, "lxc-destroy -n " + slave_name)


def run_pi_test(master):
    env.host_string = master
    env.user        = 'hadoop'
    print('execute jar file...')
    run('hadoop jar /home/hadoop/hadoop-2.6.0/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.6.0.jar pi 100 100')


def run_program(master,localpath,*options):
    """
    masterが実行したいクラスタのmasterのip
    localpathに実行したいファイルがある想定
    optionsは実行したいjarファイルのargs
    """
    #まずファイル転送
    env.host_string = '157.82.3.140'
    env.user        = 'root'
    env.key_filename ='/home/id_rsa'
    random_str      = "".join([random.choice(source_str) for x in xrange(5)])
    remotepath      = '/home/hadoop/program_' + random_str + '.jar '
    run("scp -i /home/id_rsa_600 " + localpath + " hadoop@" + master + ":" + remotepath)

    #ファイル実行
    print('execute jar file...')
    env.host_string = master
    env.user        = 'hadoop'
    run('hadoop jar ' + remotepath + " ".join(options))

    #ファイル削除
    print('delete file...')
    run('rm ' + remotepath)


if __name__ == '__main__':
    master = '157.82.3.142'
    slave1 = '157.82.3.143'
    slave2 = '157.82.3.146'
    create_cluster(master, slave1, slave2)
    run_pi_test(master)
