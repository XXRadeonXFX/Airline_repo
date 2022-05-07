###############################################################____METER_LIGHT
import string
import os
from IPython.display import clear_output
import time
import random
pos_char = "⚪️🔵🟡⚫️"
# pos_char = '🧮' + string.ascii_uppercase  +'🟥🟧🟨⬜️🟪🟦🟧🟨⬜️🟪🟦🟥🟧🟨⬜️🟪🟦  R  A  D  E  O  N  🟥🟧🟨⬜️🟪🟦' # +string.ascii_lowercase + string.ascii_uppercase + string.digits +
t = " 🟥🟧🟨⬜️🟪🟦🟧🟨⬜️🟪🟦🟥🟧🟨⬜️🟪🟦  R  A  D  E  O  N  🟥🟧🟨⬜️🟪🟦"
# t='RAD'
randomizer = "".join(random.choice(pos_char) for i in range(len(t))  )
Next_thing = ""
Gateopen = False
iterator = 0
while Gateopen ==False :
    #     print(randomizer)
    for i in range(50):
        print(randomizer*2)
    Next_thing =""
    Gateopen = True
    for i in range(len(t)):
        if randomizer[i] != t[i]:
            Gateopen = False
            Next_thing += random.choice(pos_char)
        else :
            Next_thing += t[i]
    iterator +=1
    clear_output(wait = True)
    randomizer = Next_thing
    time.sleep(0.1)
ramdomizer = random.choice(pos_char)
randomizer