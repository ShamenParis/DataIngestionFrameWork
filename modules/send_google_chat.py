# Send Messages to GChat
from json import dumps
from httplib2 import Http
import base64

class SendGChat:
    """
    This class is designed to send messages to Google Chat via a specified webhook URL.
    """
    def __init__(self,webhook_url) -> None:
        """
        Creator:
        --------
        Created by: Shamen Paris
        Email: shamen_paris@next.co.uk

        Initializes the SendGChat object with a webhook URL.

        Parameters:
        webhook_url (str): The URL of the webhook through which messages will be sent.
        """
        self.webhook_url = webhook_url
        
    def failed_message(self,table_name,error_message,job_url,current_time) -> None:
        app_message = {
            #   "text": "Here's your avatar",|
            "cardsV2": [
                {
                    "cardId": "avatarCard",
                    "card": {
                        "header": {
                            "title": "<font color=\"#FF0000\">Error Alert</font>",
                            "subtitle": f"Table: {table_name}",
                            "imageUrl": f"https://lh3.googleusercontent.com/pw/AP1GczPMgI2bx9MvOXzZbNlB0MseRb_CEGeZIsbedHqO84OeCeEoLojEyIaYaaA8GGT86ir1vFX_oH9iLm2KHNiltKtHU4KcWQ-bKbd6siDOD4xTUjQE3IPj1UR7gxbgg2y0g9WzGfuK65YbFC_6QDO9KV2GzqMtijuqb6ddncCoVXJDeHqRBs-cKsnsK73E8_V1tftq1JnUH5SBwepqQDAGqy_zATBRTNPRoJ2PS1P2KdsJ-OfNyeGK3AE72Cwg_QHVL04eJxEJmueSjblWoDT_ANNAeSwShjex13R7K73IIUnwaG6dpvj8x5EdHmytDWXUMkrqBdD_KFCZr5XYzF4ip5X32qRqshhWsKFTyErZ-_Z5cZKxNq0jMOXGZuWtWJBYW_HOh2rEWEYSF-3JzedITYMvi-dq9sj1v90VLRm8c16DzlnXTsK0Fc764n8OcwqwT02LmQS4QAJI3NFwbLyZUF6CFdUGogB9Wi6I2-NPUa7idBtM5VRKTYMOvuE-AjH37E46e_OSSUWHmXM8AcM2ujZ4K6bGA42qSdw0fPMZf6Zt6NUuWK_S7F0GG0Mr_7y764OeTS8sG8CyjwXxAWZzx8TqXSAvkgPXk41lqpU_wz1xtB1U3HAp_D9_Hp1hxLkVpJF4JJCBu5OQXR4N8WQ0Gi4U8ptWsad_RvPoD6grPSZDQQFValBZEhdy_fEU6FfhFU41HTSm9RBZ76bbkxYXGxNSrppP3IefxwlDXRZB8NpKm_cb52rRQlTPWPOgpj1Re_lBCKUIQvRHygpMlav9gGAxuUbWCaEZiBF2OhjW5tewAQLdEpegGowcprN13MWS0_PKWXPQbsqGDtUkFwFICq8SO2LA1PPHHxjjgsjzkq9yehIWAMucmM2NIyO2-XcbvWf-aaeFnLcXpiPVGLRo7Vzwcy0=w512-h512-s-no-gm?authuser=0",
                            "imageType": "CIRCLE"
                        },
                        "sections": [
                            {
                            "header": "Error Message",
                            "collapsible": True,
                            "uncollapsibleWidgetsCount": 1,
                            "widgets": [
                                {
                                "textParagraph": {
                                    "text": error_message
                                }
                                }
                            ]
                            },
                            {
                            "header": "Job URL",
                            "collapsible": True,
                            "uncollapsibleWidgetsCount": 1,
                            "widgets": [
                                {
                                "textParagraph": {
                                    "text": f"See <a href={job_url}>this link</a>"
                                }
                                }
                            ]
                            },
                            {
                            "header": "Date and Time",
                            "collapsible": True,
                            "uncollapsibleWidgetsCount": 1,
                            "widgets": [
                                {
                                "textParagraph": {
                                    "text": current_time
                                }
                                }
                            ]
                            }
                        ]
                    },
                }
            ]
        }
        message_headers = {'Content-Type': 'application/json; charset=UTF-8'}
        http_obj = Http()
        response = http_obj.request(
            uri=self.webhook_url,
            method='POST',
            headers=message_headers,
            body=dumps(app_message),
        )
        print('Message has been sent')
    def warning_message(self,table_name,message,current_time) -> None:
        app_message = {
            #   "text": "Here's your avatar",|
            "cardsV2": [
                {
                    "cardId": "avatarCard",
                    "card": {
                            "header": {
                            "title": "Alert",
                            "subtitle": f"Table: {table_name}",
                            "imageUrl": "https://lh3.googleusercontent.com/pw/AP1GczNI5OsPzJ5UsD1rK9vq39GlL1wFuVWNDbad-nUEvPDdLihqf_tPizJSw9xTCvv2EBV9tkEiVeoFDLlKtlgWqBg9TVoSD421r27g1aT9ATZXhk5zygnGNpB6UuBS2I4kQSVHtY2VT6Q5cqSDj7l--fovFcvWJbSwDpqEWdjvn-zd_vAuovkLj2NMCiRdRH-RQ4MbPeBsKdLS8_hE4XUCEWNZyJ7l1HDGNkW7AP_11rI4EwKlURf2E4Y2HjBYG6YgtiApWzI3L57N9eG_GGfIRjBlmmaI3cCGg5jeUf7euSL-2-Bs8hcdAqDVPC1cWi89kA40uJSpGIlIWwAznYVoDcaLOgWZJiPEMWN-QVZoyURkDzN_ci5KcFL9CYrp7O6y5COd_Xo_R1kgUoyngrst6AhEmqW6eAnPM_1eF_emA9hdjeE5iv8Bbz_yeA4K6jle0Cgq47HiIpO_HlN-9D5tlpA0K3pjpjUPLP3IvyzVHFqLhoEmnueC_cI3PbJFXfWeobeXjRipXJExEWCmZyro_SqZ-MGAfPblpw3q2XTHc-jmGaT9aW_a-dz6Fau3u9WlX_C5x3MMYQkec2g9F-Ddc08__C5SUPFoaGKrVJbr6h0IBuvgJLrEzNAVwNZt9OeSVCXZ5leWVF3e_GUI9FNYNAUgoYQvwYN5dDcHUizEtKvd7apPwh1FiepzK7gCdHS1FjlDnfmNaF_z0sbX85c-BmpXz7awcwAj5vc5B9Pnp9Xvlcorov8WL4Sv6S62NxUc3OuYp-QZjLmwy0kyzu-tuP3_41ByQFbqno80Qwmh-OxUxYhjBL1H7rtQailsraMapelAufjQbzEJ3DtWmT35z4mKqmpfRDaA9I9JK9mX134Eb9cyjDqYtMfSiWjOFLxdwnwQejnTclEuV9eXdN4k8ngywGo=w512-h512-s-no-gm?authuser=0",
                            "imageType": "CIRCLE"
                        },
                        "sections": [
                            {
                            "header": "Message",
                            "collapsible": True,
                            "uncollapsibleWidgetsCount": 1,
                            "widgets": [
                                {
                                "textParagraph": {
                                    "text": message
                                }
                                }
                            ]
                            },
                            {
                            "header": "Date and Time",
                            "collapsible": True,
                            "uncollapsibleWidgetsCount": 1,
                            "widgets": [
                                {
                                "textParagraph": {
                                    "text": current_time
                                }
                                }
                            ]
                            }
                        ]
                    },
                }
            ]
        }
        message_headers = {'Content-Type': 'application/json; charset=UTF-8'}
        http_obj = Http()
        response = http_obj.request(
            uri=self.webhook_url,
            method='POST',
            headers=message_headers,
            body=dumps(app_message),
        )
        print('Message has been sent')