import json
import random
import boto3
import time
from datetime import datetime
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

def get_referrer():
    x = random.randint(1, 5) * 50
    y = x + 30
    data = {
        "user_id": random.randint(x, y),
        "device_id": random.choice(["mobile", "computer", "tablet"]),
        "client_event": random.choice(
            [
                "beer_vitrine_nav",
                "beer_checkout",
                "beer_product_detail",
                "beer_products",
                "beer_selection",
                "beer_cart",
            ]
        ),
        "client_timestamp": datetime.now().isoformat()
    }
    return data

def main():
    try:
        AWS_REGION_NAME = "us-east-1"

        client = boto3.client("firehose", region_name=AWS_REGION_NAME)

        for i in range(1, 120):
            data = get_referrer()
            print(data)

            response = client.put_record(
                DeliveryStreamName='elastic-search-delivery-streams',
                Record={
                    'Data': json.dumps(data)
                }
            )

            print(response)
            time.sleep(1)  # Adding delay to prevent throttling

    except NoCredentialsError:
        print("AWS credentials not available.")
    except PartialCredentialsError:
        print("Incomplete AWS credentials.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
