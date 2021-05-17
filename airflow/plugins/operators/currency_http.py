import json
from datetime import timedelta, date

import requests
from airflow import AirflowException
from airflow.hooks.http_hook import HttpHook
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.utils.decorators import apply_defaults
import os


class CurrencyHttpOperator(SimpleHttpOperator):

    @apply_defaults
    def __init__(self, endpoint_auth, path, *args, **kwargs):
        super(CurrencyHttpOperator, self).__init__(*args, **kwargs)
        self.headers["Content-Type"] = "application/json"
        self.path = path
        self.endpoint_auth = endpoint_auth


    def execute(self, context):
        http = HttpHook('GET', http_conn_id=self.http_conn_id)
        self.log.info("Calling HTTP method")
        self.headers['Authorization'] = f"jwt {self._get_jwt_token()}"
        self.payload_data = {"date": f'{date(2021, 1, 1).strftime("%Y-%m-%d")}'}

        con = http.get_connection(self.http_conn_id)
        response = requests.get(f'{con.host}/{self.endpoint}', data=json.dumps(self.payload_data), headers=self.headers)

        # Does not work - always show "error": "Invalid JWT header",
        # response = http.run(self.endpoint,
        #                     json.dumps(self.payload_data),
        #                     self.headers,
        #                     self.extra_options)


        if self.log_response:
            self.log.info(response.text)
        if self.response_check:
            if not self.response_check(response):
                raise AirflowException("Response check returned False.")

        self.log.info(f"Respond data - {response}")
        execution_date_str = context["execution_date"].strftime("%Y-%m-%d")
        path_full = os.path.join(self.path, execution_date_str)

        self.log.info(f"Creating dir if not exists - {path_full}")
        os.makedirs(path_full, exist_ok=True)
        filename = os.path.join(path_full,'product.json')

        self.log.info(f"Creating directory if does not exist - {path_full}")
        with open(filename, mode='a+') as file:
            json.dump(response.json(), file)

        if self.xcom_push_flag:
            self.log.info(f"Push to XCOM file path - {filename}")
            return os.listdir(path_full)

    def _get_jwt_token(self):
        http_auth = HttpHook('POST', http_conn_id=self.http_conn_id)
        tv = http_auth.get_conn().auth
        tk = ('username', 'password')
        payload_auth = dict(zip(tk, tv))
        self.log.info("Calling HTTP auth method ")
        response = http_auth.run(self.endpoint_auth,
                            json.dumps(payload_auth),
                            self.headers,
                            self.extra_options)

        if self.response_check:
            if not self.response_check(response):
                raise AirflowException("Response check returned False.")
        resp_json = response.json()
        return resp_json['access_token']
