# ----------------------- Update in Real Time -----------------------
import time, datetime
import cv2
from time import sleep
while True:
    import time, datetime
    import cv2
    from time import sleep

    t = 1

    sleep(t)

    time_in_seconds_bar = 86400

    last_bar_time = datetime.datetime.now()

    next_bar_time = last_bar_time + datetime.timedelta(seconds=time_in_seconds_bar)
    wait_for_calculated = int((next_bar_time - datetime.datetime.now()).total_seconds())
    print("Last bar time: %s Next bar time: %s" % (last_bar_time, next_bar_time))
    print("waiting %s seconds..." % (wait_for_calculated))
    # cv2.waitKey(abs(wait_for_calculated*1000+500)) # 500 milsec delay
    for sec in range(abs(wait_for_calculated)):
        if ((sec + 1) % 30 == 0):
            print(wait_for_calculated - sec)
        else:
            print(wait_for_calculated - sec, end=" ")
        sleep(t)


