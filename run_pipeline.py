from dotenv import load_dotenv
load_dotenv()

import sys
sys.path.insert(0, '.')

from flows.pipeline_flow import airpulse_pipeline
airpulse_pipeline()
