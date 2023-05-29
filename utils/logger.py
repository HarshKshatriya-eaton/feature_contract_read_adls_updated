
# -*- coding: utf-8 -*-
"""
@file spark_self.py Class for log4j logging for SPARK Applications.

@brief Use this for logging SPARK applications.

@details

@copyright 2022 Eaton Corporation. All Rights Reserved.
@note Eaton Corporation claims proprietary rights to the material disclosed
here on. This technical information may not be reproduced or used without
direct written permission from Eaton Corporation.
"""
import logging


class AppLogger():
    """ Logging Class."""

    def __init__(self, app_name="CapEoUL", mode=0, level='Info'):
        """


        Parameters
        ----------
        app_name : TYPE, optional
            DESCRIPTION. The default is "TEST_APP".
        mode : int, optional
            0 : Local
            1 : Cloud

        Returns
        -------
        None.

        """
        self.status = {'F': 'FAILED', 'S': 'SUCCEEDED',
                       'E': 'ENDED', 'B': 'STARTED'}
        self.log_idx = 0
        self.sub_log_idx = 0
        self.app_name = app_name

        if mode == 0:
            logging.basicConfig()
            self.logger = logging.getLogger(app_name)
            if level == 'Info':
                self.logger.setLevel(logging.INFO)
            else:
                self.logger.setLevel(logging.DEBUG)

        else:
            log4jlogger = logging.basicConfig()
            self.logger = log4jlogger.LogManager.getLogger(app_name)
            self.logger.setLevel(log4jlogger.Level.INFO)

    def app_success(self, log_txt, status='S'):
        """Log application success.

        Parameters
        ----------
        log_txt : String
            Logging message.
        status : String, optional
            Logging status. The default is 'S'.

        Returns
        -------
        None.

        """
        if status in self.status:
            log_status = self.status[status]
        else:
            log_status = status

        self.logger.info(
            ' : '.join(
                [' STEP ' + str(self.log_idx),
                 log_txt, log_status]
            )
        )
        self.log_idx += 1

    def app_fail(self, log_txt, ex, status='F'):
        """Log application failure.

        Parameters
        ----------
        log_txt : String
            Logging message.
        ex : Exception
        status : String, optional
            Logging status. The default is 'F'.

        Returns
        -------
        None.

        """
        if status in self.status:
            log_status = self.status[status]
        else:
            log_status = status

        self.logger.error(
            ' : '.join(
                [' STEP ' + str(self.log_idx),
                 log_txt, log_status]
            )
        )
        self.logger.error("{}".format(ex))
        self.log_idx += 1

    def app_debug(self, log_txt, level=0):
        """Log application failure.

        Parameters
        ----------
        log_txt : String
            Logging message.
        ex : Exception
        status : String, optional
            Logging status. The default is 'F'.

        Returns
        -------
        None.

        """
        # self.log_idx += 1

        msg = ' : '.join(
            [' STEP ' + str(self.log_idx)]
            )
        n_spaces = 3 * level + len(msg)

        if level > 0:
            # self.sub_log_idx += 1
            self.logger.debug(
                ' : '.join([n_spaces * " ",
                            str(self.sub_log_idx),
                            log_txt])
                )
        else:
            self.logger.debug(
                    ' : '.join([n_spaces * " ", log_txt]))

    def app_info(self, log_txt, level=0):
        """Log application failure.

        Parameters
        ----------
        log_txt : String
            Logging message.
        ex : Exception
        status : String, optional
            Logging status. The default is 'F'.

        Returns
        -------
        None.

        """
        # self.log_idx += 1

        msg = ' : '.join(
            [' STEP ' + str(self.log_idx)]
            )
        n_spaces = 3 * level + len(msg)

        if level == 1:
            self.sub_log_idx += 1
            self.logger.info(
                ' : '.join([n_spaces * " ", str(self.sub_log_idx).zfill(2),
                            log_txt]))
        else:
            self.logger.info(
                ' : '.join([n_spaces * " ", log_txt]))

    def app_reset_ix(self, type='sub'):
        """
        Resets index.

        Parameters
        ----------
        type : int. The default is 'sub'.

        Returns
        -------
        None.

        """
        if type == 'sub':
            self.sub_log_idx = 0
        else:
            self.log_idx = 0
