import sys
import time
import json
import requests


from .database import export as db
from .secret import hosts


#Main routine to push data to remote hosts.

#Send json-encoded payload in post body of the request.
#Authorization via 'access_key' in the body.
#HTTPS should be used for security.


def post_data(url, payload):
    '''Send post request with payload to the specified url.
    Makes 2 attempts. Prints output to stdout.

    Accepts HTTP JSON response:
        {
            required "error":None/str - None if no error. String with error message in case of error.
            optional "time:float      - time took to execute request. Omitted of error is not None.
        }

    Arguments:
        url:str - endpoint to send payload to.
        payload:Dict[str, Obj] - dictionary to be sent as json payload. Must include following fields:
            name:str          - name of the object.
            headers:List[str] - headers of items in 'rows' field.
            rows:List[List]   - actual data as list of lists.
            count:int         - count of the items in the 'rows' field for basic validation.
            access_key:str    - secret access key for security purpose.
    Returns:
        None
    '''

    start = time.time()

    attempts, max_attempts = 0, 2
    while attempts < max_attempts:
        try:
            resp = requests.post(url, timeout=15, json=payload).json()
            error = resp['error']
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
        headers, rows = db.export_tankopedia()
        payload = {
            'name':       'tankopedia',
            'headers':    headers,
            'rows':       rows,
            'count':      len(rows),
            'access_key': access_key
        }
        post_data(url, payload)


        #Continue only on monday.
        if time.gmtime().tm_wday != 0:
            return


        #Percentiles.
        print('INFO: Pushing percentiles...')
        headers, rows = db.export_percentiles()
        payload = {
            'name':       'percentiles',
            'headers':    headers,
            'rows':       rows,
            'count':      len(rows),
            'access_key': access_key
        }
        post_data(url, payload)


        #Percentiles generic.
        print('INFO: Pushing percentiles generic...')
        headers, rows = db.export_percentiles_generic()
        payload = {
            'name':       'percentiles_generic',
            'headers':    headers,
            'rows':       rows,
            'count':      len(rows),
            'access_key': access_key
        }
        post_data(url, payload)


        #WN8.
        print('INFO: Pushing WN8...')
        headers, rows = db.export_wn8_exp_values()
        payload = {
            'name':       'wn8',
            'headers':    headers,
            'rows':       rows,
            'count':      len(rows),
            'access_key': access_key
        }
        post_data(url, payload)


        #History.
        print('INFO: Pushing history...')
        twenty_weeks_ago = time.time() - 60 * 60 * 24 * 7 * 20
        headers, rows = db.export_history(min_timestamp=twenty_weeks_ago)
        payload = {
            'name':       'history',
            'headers':    headers,
            'rows':       rows,
            'count':      len(rows),
            'access_key': access_key
        }
        post_data(url, payload)


if __name__ == '__main__':
    main()
