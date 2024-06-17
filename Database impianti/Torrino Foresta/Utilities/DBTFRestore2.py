import pandas as pd
import numpy as np

df = pd.read_csv("DBTFNEW4.csv")

PLC1_AI_PT_TURBINA = df["PLC1_AI_PT_TURBINA"]

TEST = PLC1_AI_PT_TURBINA
# TEST = float(PLC1_AI_PT_TURBINA.replace(",", "."))

TEST_REV = TEST
TEST_REV[TEST>4] = TEST[TEST > 4]/10.1974

PLC1_AI_PT_TURBINA = TEST_REV

dfOut = df
dfOut["PLC1_AI_PT_TURBINA"] = PLC1_AI_PT_TURBINA
dfOut.to_csv("DBTFNEW4_Test.csv", index=False)
