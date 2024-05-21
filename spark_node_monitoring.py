import subprocess

def get_available_space():
  def get_hostname_and_space():
    hostname = subprocess.check_output("hostname",shell=True).decode().strip()

    df_output = subprocess.check_output("df /local_disk0/",shell=True).decode().split()
    space_in_kb = int(df_output[10])
    space_in_gb = "{:.2f}".format(space_in_kb/1024/1024)

  
