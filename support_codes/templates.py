
"""@file



@brief


@details


@copyright 2021 Eaton Corporation. All Rights Reserved.
@note Eaton Corporation claims proprietary rights to the material disclosed
here on. This technical information may not be reproduced or used without
direct written permission from Eaton Corporation.
"""

# ***** XXX *****
_step = ''
try:

    ENV_.logger.app_success(_step)
except Exception as e:
    ENV_.logger.app_fail(_step, f"{traceback.print_exc()}")
    raise Exception from e
