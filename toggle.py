import RPi.GPIO as gpio
import time
from signal import signal, SIGINT
from sys import exit
import os
import subprocess

state = len(os.listdir("hddlock"))<=0
def handler(signal_received, frame):
    # on gère un cleanup propre
    print('')
    print('SIGINT or CTRL-C detected. Exiting gracefully')
    turn_off()
    gpio.cleanup()
    exit(0)

def main():
    global state
    # on passe en mode BMC qui veut dire que nous allons utiliser directement
    # le numero GPIO plutot que la position physique sur la carte
    gpio.setmode(gpio.BCM)

    # defini le port GPIO 4 comme etant une sortie output
    gpio.setup(4, gpio.OUT)

    # Mise a 1 pendant 2 secondes puis 0 pendant 2 seconde
    while True:
        time.sleep(5)
        delete_old_locks()
        if os.path.isdir("hddlock") and len(os.listdir("hddlock"))>0:
            if(state != "on"):
                turn_on()
        elif state != "off":
            turn_off()

def delete_old_locks():
    current = time.time()
    if os.path.isdir("hddlock"):
        for name in os.listdir("hddlock"):
            path = os.path.join("hddlock", name)
            if(name.startswith("released_")):
                try:
                    if(current - os.path.getmtime(path) > 30):
                
                        print("more than 30")
                        os.remove(path)
                except Exception as e:
                    print("oopsi")

def turn_on():
    global state
    print("on")
    gpio.output(4, gpio.LOW)
    mount_all()
    state = "on"


def turn_off():
    global state
    print("off")
    umount_all()
    gpio.output(4, gpio.HIGH)
    state = "off"

def umount(path):
    cmd = 'umount -l ' + path
    proc = subprocess.Popen(str(cmd), shell=True, stdout=subprocess.PIPE).stdout.read()
    print(proc)

def mount(dev, path):
    cmd = 'mount '+dev+" " + path
    proc = subprocess.Popen(str(cmd), shell=True, stdout=subprocess.PIPE).stdout.read()
    print(proc)

def mount_all():
    while (not os.path.ismount("/mnt/data2") or not os.path.ismount("/mnt/data3")):
        mount("/dev/disk/by-uuid/5c3f23ea-e614-4a23-a060-97353cd55a10","/mnt/data2")
        mount("/dev/disk/by-uuid/03e42165-4725-4667-81e0-e1aa849fecb3", "/mnt/data3")
        time.sleep(1)
    
    
def umount_all():
    umount("/mnt/data2")
    umount("/mnt/data3")
    


if __name__ == '__main__':
    # On prévient Python d'utiliser la method handler quand un signal SIGINT est reçu
    signal(SIGINT, handler)
    main()
