import os
import cv2
import boto3
import uuid
import time

from time import strftime, localtime
from picamera.array import PiRGBArray
from picamera import PiCamera

# :: mimic device ID
device_id = uuid.uuid4().hex

# :: subprocesses
upload_tasks = []

# :: AWS
bucketname = os.environ.get('BUCKET_NAME', 'test-bucket')
s3_client = boto3.client('s3')

# :: Camera
camera = PiCamera()
camera.resolution = (1280, 1024)
camera.framerate = 32
rawcapture = PiRGBArray(camera, size=(1280, 1024))

poll_interval = int(os.environ.get('CAPTURE_INTERVAL', 15))

time.sleep(0.5)

# :: ---

timestamp = int(time.time())

# while True:
for frame in camera.capture_continuous(rawcapture, format="bgr", use_video_port=True):
  newtime = int(time.time())
  delta = newtime - timestamp

  if delta > poll_interval:
    image = frame.array
    
    filename = strftime("%Y%m%d%H%M%S", localtime()) + '_' + device_id + '.png'
    cv2.imwrite(filename, image)

    # :: upload the file to S3
    task = os.fork()

    if (task == 0):
      s3_client.upload_file(filename, bucketname, filename)
      print ('>> file %s uploaded to S3' % filename)
      os.remove(filename)
      os._exit(0)
    else:
      print ('>> child forked, pid = %d' % task)
      upload_tasks.append(task)

    timestamp = newtime

  rawcapture.truncate(0)

  # :: manage tasks
  for upload_task in upload_tasks:
    pid, sts = os.waitpid(upload_task, os.WNOHANG)
    if (pid == upload_task):
      upload_tasks.remove(upload_task)

cv2.destroyAllWindows()
