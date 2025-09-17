import ftplib
import ssl

class ImplicitFTP_TLS(ftplib.FTP_TLS):
    """FTP_TLS subclass that automatically wraps sockets in SSL to support implicit FTPS."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._sock = None

    @property
    def sock(self):
        """Return the socket."""
        return self._sock

    @sock.setter
    def sock(self, value):
        """When modifying the socket, ensure that it is ssl wrapped."""
        if value is not None and not isinstance(value, ssl.SSLSocket):
            value = self.context.wrap_socket(value)
        self._sock = value


def connect_to_ftp():
    ftp_client = ImplicitFTP_TLS()
    ftp_client.connect(host='download.ipex.it', port=990)
    ftp_client.login(user='DAMIANOZILIO', passwd='O12L10Z1')
    ftp_client.prot_p()
    ftp_client.cwd('MercatiElettrici/MGP_Prezzi')

    return ftp_client

# filename = '20250906MGPPrezzi.xml'

# with open( filename, 'wb' ) as file :
#         ftp_client.retrbinary('RETR %s' % filename, file.write)

# df = pd.read_xml(filename)
# print(df)

# giorno = pd.to_datetime(df["Data"][1:].astype(int).astype(str),format="%Y%m%d")
# ore = df["Ora"]