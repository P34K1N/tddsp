import redis
import random
import json
import shutil

TAG_ORDER = ['brand', 'type']

def etl_redis(id, image_data):
    r = redis.Redis(port=6379 + id)
    image_dir = './images_{}'.format(id)
    for image, tags in image_data.items():
        shutil.copyfile('./images_raw/{}'.format(image), '{}/{}'.format(image_dir, image))
        idx = 0
        while True:
            tags_formatted = ['{0}-{1}'.format(cat, value) for cat, value in zip(TAG_ORDER, tags)]
            key_attempt = ':'.join(tags_formatted + [str(idx)])
            if r.get(key_attempt) is None:
                r.set(key_attempt, image)
                print(key_attempt, image)
                break
            idx += 1

image_data = json.load(open('./images_raw/image_to_tag.json'))
images = list(image_data.keys())
random.shuffle(images)
image_data_0, image_data_1 = {}, {}
for image in images[:len(images)//2]:
    image_data_0[image] = image_data[image]
for image in images[len(images)//2:]:
    image_data_1[image] = image_data[image]

etl_redis(0, image_data_0)
etl_redis(1, image_data_1)
