from airflow.models import DAG 
from datetime import datetime
from airflow.decorators import task
from sheet2api import Sheet2APIClient
import pandas as pd
import json
from airflow.operators.python import PythonOperator

spend_sheet = Sheet2APIClient(api_url = 'https://sheet2api.com/v1/lSkMlXOdT5aS/denver_house_spending_2022/')
ted_budget = Sheet2APIClient(api_url = 'https://sheet2api.com/v1/lSkMlXOdT5aS/teddy_budget_2022')

months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']


default_args = {
	'start_date': datetime(2020, 1, 1)
}

def _get_data_groc(**kwargs):
	ti = kwargs['ti']
	data_groc = spend_sheet.get_rows(sheet = 'August', query={'Category':'Groceries'})

	prices = []
	if len(data_groc) == 1:
		Grocery_Sum = data_groc['Amount']
	elif len(data_groc) > 1:
		for d in data_groc:
			prices.append(d['Amount'])
		Grocery_Sum = sum(prices)/3
	else:
		Grocery_Sum = 0

	ti.xcom_push(key = 'groceries_pull', value = Grocery_Sum)

def _get_data_util(**kwargs):
	ti = kwargs['ti']
	data_util = spend_sheet.get_rows(sheet = 'August', query={'Category':'Utilities'})

	prices = []
	if len(data_util) == 1:
		Utilities_Sum = data_util[0]['Amount']/3
	elif len(data_util) > 1:
		for d in data_util:
			prices.append(d['Amount'])
		Utilities_Sum = sum(prices)/3
	else:
		Utilities_Sum = 0

	ti.xcom_push(key = 'utilities_pull', value = Utilities_Sum)


def _update_budget(**kwargs):
	ti = kwargs['ti']
	Grocery_Sum = ti.xcom_pull(key = 'groceries_pull', task_ids = ['get_data_groc'])
	Grocery_Sum = int(Grocery_Sum[0])
	Utilities_Sum = ti.xcom_pull(key = 'utilities_pull', task_ids = ['get_data_util'])
	Utilities_Sum = int(Utilities_Sum[0])

	print(Grocery_Sum)
	print(Utilities_Sum)

	ted_budget.update_rows(
		sheet = 'Data_Load',
		query = {'month': 'August'},
		row = {
			'groceries': Grocery_Sum,
			'utilities': Utilities_Sum
		},
		partial_update = True,
	)











with DAG(dag_id = 'check_dag', schedule_interval = '@daily', default_args = default_args, catchup=False) as dag:

	get_data_groc = PythonOperator(
		task_id = 'get_data_groc',
		python_callable = _get_data_groc
	)

	get_data_util = PythonOperator(
		task_id = 'get_data_util',
		python_callable = _get_data_util
	)

	update_spreadsheet = PythonOperator(
		task_id = 'update_spreadsheet',
		python_callable = _update_budget
	)

	
	[get_data_groc, get_data_util] >> update_spreadsheet