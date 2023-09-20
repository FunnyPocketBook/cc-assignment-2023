import requests

headers = {
    'accept': 'text/plain; charset=utf-8',
    'Content-Type': 'application/json; charset=utf-8',
}

json_data = {
    'notification_type': 'OutOfRange',
    'researcher': 'd.landau@uu.nl',
    'measurement_id': '1234',
    'experiment_id': '5678',
    'cipher_data': 'D5qnEHeIrTYmLwYX.hSZNb3xxQ9MtGhRP7E52yv2seWo4tUxYe28ATJVHUi0J++SFyfq5LQc0sTmiS4ILiM0/YsPHgp5fQKuRuuHLSyLA1WR9YIRS6nYrokZ68u4OLC4j26JW/QpiGmAydGKPIvV2ImD8t1NOUrejbnp/cmbMDUKO1hbXGPfD7oTvvk6JQVBAxSPVB96jDv7C4sGTmuEDZPoIpojcTBFP2xA',
}

response = requests.post('http://notif:3000/api/notify', headers=headers, json=json_data)

print(response.text)

with open('data/response.txt', 'a') as f:
    f.write(response.text + '\n')