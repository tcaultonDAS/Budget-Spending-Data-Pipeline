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
months_spend = ['July', 'August', 'September', 'October', 'November', 'December']

grocery_data = {'January':0, 'February':0, 'March':0, 'April':0, 'May':0, 'June':0, 'July':0, 'August':0, 'September':0, 'October':0, 'November':0, 'December':0}
utility_data = {'January':0, 'February':0, 'March':0, 'April':0, 'May':0, 'June':0, 'July':0, 'August':0, 'September':0, 'October':0, 'November':0, 'December':0}

default_args = {
	'start_date': datetime(2020, 1, 1)
}

def _get_data_groc(**kwargs):
	ti = kwargs['ti']

	for i in months_spend:
		data_groc = spend_sheet.get_rows(sheet = i, query={'Category':'Groceries'})

		prices = []
		if len(data_groc) == 1:
			Grocery_Sum = data_groc['Amount']
			grocery_data[i] = Grocery_Sum
		elif len(data_groc) > 1:
			for d in data_groc:
				prices.append(d['Amount'])
			Grocery_Sum = sum(prices)/3
			grocery_data[i] = Grocery_Sum
		else:
			Grocery_Sum = 0
			grocery_data[i] = Grocery_Sum

	ti.xcom_push(key = 'groceries_pull', value = grocery_data)
	print(grocery_data['July'])

def _get_data_util(**kwargs):
	ti = kwargs['ti']

	for i in months_spend:
		data_util = spend_sheet.get_rows(sheet = i, query={'Category':'Utilities'})

		prices = []
		if len(data_util) == 1:
			Utilities_Sum = data_util[0]['Amount']/3
			utility_data[i] = Utilities_Sum
		elif len(data_util) > 1:
			for d in data_util:
				prices.append(d['Amount'])
			Utilities_Sum = sum(prices)/3
			utility_data[i] = Utilities_Sum
		else:
			Utilities_Sum = 0
			utility_data[i] = Utilities_Sum

	ti.xcom_push(key = 'utilities_pull', value = utility_data)


def _update_budget(**kwargs):
	ti = kwargs['ti']

	grocery_data = ti.xcom_pull(key = 'groceries_pull', task_ids = ['get_data_groc'])
	utility_data = ti.xcom_pull(key = 'utilities_pull', task_ids = ['get_data_util'])
	print(grocery_data)
	print(utility_data)

	for i in months:
		Grocery_Sum = grocery_data[0][i]
		Grocery_Sum = int(Grocery_Sum)

		Utilities_Sum = utility_data[0][i]
		Utilities_Sum = int(Utilities_Sum)

		ted_budget.update_rows(
			sheet = 'Data_Load',
			query = {'month': i},
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