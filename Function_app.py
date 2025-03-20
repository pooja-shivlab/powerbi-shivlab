import azure.functions as func
import logging
from Parent_ESG import ParentESGFunctionIndex
from Parent_Finance import ParentFinanceFunctionIndex
from Parent_Risk import ParentRiskFunctionIndex
from Parent_Subsidiary_Operational import ParentSubsidiaryOPFunctionIndex
from Subsidiary_ESG import SubsidiaryESGFunctionIndex
from Subsidiary_Finance import SubsidiaryFinanceFunctionIndex
from Subsidiary_Risk import SubsidiaryRiskFunctionIndex
from Master_Configuration import MasterConfigurationFunctionIndex

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)


@app.timer_trigger(schedule="0 0 1 * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def ParentESGTimerFunction(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    ParentESGFunctionIndex()

    logging.info('Python timer trigger function executed.')

logging.basicConfig(level=logging.DEBUG, format='%(levelname)s: %(message)s')


@app.timer_trigger(schedule="0 3 1 * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def ParentFinanceTimerFunction(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    ParentFinanceFunctionIndex()

    logging.info('Python timer trigger function executed.')

logging.basicConfig(level=logging.DEBUG, format='%(levelname)s: %(message)s')



@app.timer_trigger(schedule="0 6 1 * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def ParentRiskTimerFunction(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    ParentRiskFunctionIndex()

    logging.info('Python timer trigger function executed.')

logging.basicConfig(level=logging.DEBUG, format='%(levelname)s: %(message)s')



@app.timer_trigger(schedule="0 9 1 * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def SubsidiaryESGTimerFunction(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    SubsidiaryESGFunctionIndex()

    logging.info('Python timer trigger function executed.')

logging.basicConfig(level=logging.DEBUG, format='%(levelname)s: %(message)s')



@app.timer_trigger(schedule="0 12 1 * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def SubsidiaryFinanceTimerFunction(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    SubsidiaryFinanceFunctionIndex()

    logging.info('Python timer trigger function executed.')

logging.basicConfig(level=logging.DEBUG, format='%(levelname)s: %(message)s')



@app.timer_trigger(schedule="0 15 1 * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def SubsidiaryRiskTimerFunction(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    SubsidiaryRiskFunctionIndex()

    logging.info('Python timer trigger function executed.')

logging.basicConfig(level=logging.DEBUG, format='%(levelname)s: %(message)s')



@app.timer_trigger(schedule="0 18 1 * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def ParentSubsidiaryOPTimerFunction(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    ParentSubsidiaryOPFunctionIndex()

    logging.info('Python timer trigger function executed.')

logging.basicConfig(level=logging.DEBUG, format='%(levelname)s: %(message)s')



@app.timer_trigger(schedule="0 21 1 * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def MasterConfigurationTimerFunction(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    MasterConfigurationFunctionIndex()

    logging.info('Python timer trigger function executed.')

logging.basicConfig(level=logging.DEBUG, format='%(levelname)s: %(message)s')