import sys
import time
import json
import requests


from .database import export as db
from .secret import hosts


#Main routine to push data to remote hosts.

#Send json-encoded payload in post body of the request.
#Authorization via 'access_key' in the body.
#HTTPS must be used for security.


def post_data(url, payload):
    '''Send post request with payload to the specified url.

    Makes 3 attempts. Prints output to stdout.
    Reciving API must respond with JSON containing 'status' and 'message' fields.
    Response field 'status' must be a string containing 'ok' if no errors encountered.
    If response field 'status' is not 'ok', request considered to be a failure.

    Arguments:
        url:str - endpoint to send payload to.
        payload:Dict[str, Obj] - dictionary to be sent as json payload. Must include following fields:
            name:str       - name of the object.
            data:List[Obj] - main data to be received, processed & stored.
            count:int      - count of the items in the 'data' field for basic validation.
            access_key:str - secret access key for security purpose.
    Returns:
        None
    '''

    start = time.time()

    attempts, max_attempts = 0, 2
    while attempts < max_attempts:
        try:
            resp = requests.post(url, timeout=15, json=payload).json()
            error =  resp['error']
            assert error is None, error

        except requests.exceptions.Timeout:
            print('ERROR: request timeout')
            attempts += 1

        except (requests.exceptions.InvalidSchema, requests.exceptions.ConnectionError):
            print('ERROR: connection error, check supplied URL')
            attempts += 1

        except json.decoder.JSONDecodeError:
            print('ERROR: Cant decode json')
            attempts += 1

        except AssertionError as e:
            print('ERROR:', e)
            attempts += 1

        except KeyError:
            print('ERROR: json response doesnt contain \'status\' or \'message\' keys')
            attempts += 1

        else:
            #Feedback.
            took = round(time.time() - start, 2)
            kib = round(sys.getsizeof(json.dumps(payload)) / 1024, 2)
            print(f'SUCCESS: Took: {took} s. Size: {kib} KiB')
            return


def main():
    #Iterate through hosts and send pieces of data.

    if not any(hosts):
        print('WARNING: No hosts found. Data will not be sent anywhere.')


    for host in hosts:

        url, access_key = host['url'], host['access_key']
        print(f'INFO: Sending data to {url}')


        #Tankopedia.
        print('INFO: Pushing tankopedia...')
        data = db.export_tankopedia()
        payload = {
            'name':       'tankopedia',
            'data':       data,
            'count':      len(data),
            'access_key': access_key
        }
        post_data(url, payload)


        #Continue only on monday.
        if time.gmtime().tm_wday != 0:
            return


        #Percentiles.
        print('INFO: Pushing percentiles...')
        data = db.export_percentiles()
        payload = {
            'name':       'percentiles',
            'data':       data,
            'count':      len(data),
            'access_key': access_key
        }
        post_data(url, payload)


        #Percentiles generic.
        print('INFO: Pushing percentiles generic...')
        data = db.export_percentiles_generic()
        payload = {
            'name':       'percentiles_generic',
            'data':       data,
            'count':      len(data),
            'access_key': access_key
        }
        post_data(url, payload)


        #WN8.
        print('INFO: Pushing WN8...')
        data = db.export_wn8_exp_values()
        payload = {
            'name':       'wn8',
            'data':       data,
            'count':      len(data),
            'access_key': access_key
        }
        post_data(url, payload)


        #History.
        print('INFO: Pushing history...')
        data = db.export_history()
        payload = {
            'name':       'history',
            'data':       data,
            'count':      len(data),
            'access_key': access_key
        }
        post_data(url, payload)


if __name__ == '__main__':
    main()
