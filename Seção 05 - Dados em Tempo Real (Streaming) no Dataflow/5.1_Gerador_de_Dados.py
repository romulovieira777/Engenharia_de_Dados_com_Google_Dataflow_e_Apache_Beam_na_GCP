#pip install google-cloud-pubsub
#producer

import csv
import time
from google.cloud import pubsub_v1
import os

service_account_key = r" "
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_account_key

topico = ' '
publisher = pubsub_v1.PublisherClient()

entrada = r" "

with open(entrada, 'rb') as file:
    for row in file:
        print('Publishing in Topic')
        publisher.publish(topico, row)
        time.sleep(2)
