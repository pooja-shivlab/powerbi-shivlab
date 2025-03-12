import azure.functions as func
import logging
from financeRisk import FinanceRiskFunctionIndex
from operationESG import OperationESGFunctionIndex
from Parent_ESG import ParentESGFunctionIndex
from Parent_Finance import ParentFinanceFunctionIndex
from Parent_Risk import ParentRiskFunctionIndex
from Subsidiary_ESG import SubsidiaryESGFunctionIndex
from Subsidiary_Finance import SubsidiaryFinanceFunctionIndex
from Subsidiary_Risk import SubsidiaryRiskFunctionIndex
from Parent_Subsidiary_Operational import ParentSubsidiaryOPFunctionIndex
from Master_Configuration import MasterConfigurationFunctionIndex


app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)


@app.timer_trigger(schedule="0 0 1 * * *", arg_name="myTimer", run_on_startup=True,
                   use_monitor=False)
def OperationsAndESGTimerFunction(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    OperationESGFunctionIndex()
    logging.info('Python timer trigger function executed.')


@app.timer_trigger(schedule="0 0 1 * * *", arg_name="myTimer", run_on_startup=True,
                   use_monitor=False)
def FinanceRiskTimerFunction(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    FinanceRiskFunctionIndex()

    logging.info('Python timer trigger function executed.')


logging.basicConfig(level=logging.DEBUG, format='%(levelname)s: %(message)s')


def ParentESGTimerFunction(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    ParentESGFunctionIndex()

    logging.info('Python timer trigger function executed.')

logging.basicConfig(level=logging.DEBUG, format='%(levelname)s: %(message)s')

def ParentFinanceTimerFunction(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    ParentFinanceFunctionIndex()

    logging.info('Python timer trigger function executed.')

logging.basicConfig(level=logging.DEBUG, format='%(levelname)s: %(message)s')

def ParentRiskTimerFunction(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    ParentRiskFunctionIndex()

    logging.info('Python timer trigger function executed.')

logging.basicConfig(level=logging.DEBUG, format='%(levelname)s: %(message)s')

def SubsidiaryESGTimerFunction(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    SubsidiaryESGFunctionIndex()

    logging.info('Python timer trigger function executed.')

logging.basicConfig(level=logging.DEBUG, format='%(levelname)s: %(message)s')


def SubsidiaryFinanceTimerFunction(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    SubsidiaryFinanceFunctionIndex()

    logging.info('Python timer trigger function executed.')

logging.basicConfig(level=logging.DEBUG, format='%(levelname)s: %(message)s')

def SubsidiaryRiskTimerFunction(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    SubsidiaryRiskFunctionIndex()

    logging.info('Python timer trigger function executed.')

logging.basicConfig(level=logging.DEBUG, format='%(levelname)s: %(message)s')

def ParentSubsidiaryOPTimerFunction(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    ParentSubsidiaryOPFunctionIndex()

    logging.info('Python timer trigger function executed.')

logging.basicConfig(level=logging.DEBUG, format='%(levelname)s: %(message)s')

def MasterConfigurationTimerFunction(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    MasterConfigurationFunctionIndex()

    logging.info('Python timer trigger function executed.')

logging.basicConfig(level=logging.DEBUG, format='%(levelname)s: %(message)s')