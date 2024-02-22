from colorama import Fore, Style


def displayState(State):

    if State == "O":
        print(f'-- {Fore.GREEN}In produzione!{Style.RESET_ALL}')

    elif State == "OK":
        print(f'-- {Fore.GREEN}In produzione!{Style.RESET_ALL}')

    elif State == "W":
        print(f'-- {Fore.YELLOW}In no link!{Style.RESET_ALL}')

    elif State == "U":
        print(f'-- {Fore.YELLOW}Stato centrale non riconosciuto!{Style.RESET_ALL}')

    else:
        print(f'-- {Fore.RED}ANOMALIE CENTRALE RILEVATA!{Style.RESET_ALL}')