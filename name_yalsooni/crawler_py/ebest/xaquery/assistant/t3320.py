from name_yalsooni.crawler_py.ebest.xaquery.assistant.abstract import DBResponseAssistant, ResponseAssistant, Assistant
from name_yalsooni.crawler_py.ebest.xaquery.command.abstract import CM
from name_yalsooni.crawler_py.ebest.xaquery.command.t3320 import T3320Command
from name_yalsooni.crawler_py.ebest.util import Log


class T3320Assistant(Assistant):

    def __init__(self):
        super(T3320Assistant, self).__init__(T3320Command.TR_NAME, 3)
        self._response_dict[CM.RES_TYPE_DB] = T3320DBInsert()
        self._response_dict[CM.RES_TYPE_SOCKET] = T3320Socket()

    def request_process(self, command):

        return True


class T3320DBInsert(DBResponseAssistant):

    T3320_A_INSERT = "INSERT INTO COL_EB_T3320_A ( REQDT, SHCODE, UPGUBUNNM, SIJANGCD, MARKETNM, COMPANY, BADDRESS, BTELNO, GSYYYY, GSMM, GSYM, LSTPRICE, GSTOCK, HOMEURL, GRDNM, FOREIGNRATIO, IRTEL, CAPITAL, SIGAVALUE, CASHSIS, CASHRATE, PRICE, JNILCLOSE) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )"
    T3320_B_INSERT = "INSERT INTO COL_EB_T3320_B ( REQDT, SHCODE, GICODE, GSYM, GSGB, PER, EPS, PBR, ROA, ROE, EBITDA, EVEBITDA, PAR, SPS, CPS, BPS, T_PER, T_EPS, PEG, T_PEG, T_GSYM) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )"

    def db_response_process(self, command, tr, request_date, curs):

        try:
            curs.execute(self.T3320_A_INSERT, (
                command.get_request_date(),
                command.get_shcode(),
                tr.GetFieldData("t3320OutBlock", "upgubunnm", 0),
                tr.GetFieldData("t3320OutBlock", "sijangcd", 0),
                tr.GetFieldData("t3320OutBlock", "marketnm", 0),
                tr.GetFieldData("t3320OutBlock", "company", 0),
                tr.GetFieldData("t3320OutBlock", "baddress", 0),
                tr.GetFieldData("t3320OutBlock", "btelno", 0),
                tr.GetFieldData("t3320OutBlock", "gsyyyy", 0),
                tr.GetFieldData("t3320OutBlock", "gsmm", 0),
                tr.GetFieldData("t3320OutBlock", "gsym", 0),
                tr.GetFieldData("t3320OutBlock", "lstprice", 0),
                tr.GetFieldData("t3320OutBlock", "gstock", 0),
                tr.GetFieldData("t3320OutBlock", "homeurl", 0),
                tr.GetFieldData("t3320OutBlock", "grdnm", 0),
                tr.GetFieldData("t3320OutBlock", "foreignratio", 0),
                tr.GetFieldData("t3320OutBlock", "irtel", 0),
                tr.GetFieldData("t3320OutBlock", "capital", 0),
                tr.GetFieldData("t3320OutBlock", "sigavalue", 0),
                tr.GetFieldData("t3320OutBlock", "cashsis", 0),
                tr.GetFieldData("t3320OutBlock", "cashrate", 0),
                tr.GetFieldData("t3320OutBlock", "price", 0),
                tr.GetFieldData("t3320OutBlock", "jnilclose", 0)
            )
                         )
        except Exception as ex:
            Log.write(str(ex))

        try:
            curs.execute(self.T3320_B_INSERT, (
                command.get_request_date(),
                command.get_shcode(),
                tr.GetFieldData("t3320OutBlock1", "gicode", 0),
                tr.GetFieldData("t3320OutBlock1", "gsym", 0),
                tr.GetFieldData("t3320OutBlock1", "gsgb", 0),
                tr.GetFieldData("t3320OutBlock1", "per", 0),
                tr.GetFieldData("t3320OutBlock1", "eps", 0),
                tr.GetFieldData("t3320OutBlock1", "pbr", 0),
                tr.GetFieldData("t3320OutBlock1", "roa", 0),
                tr.GetFieldData("t3320OutBlock1", "roe", 0),
                tr.GetFieldData("t3320OutBlock1", "ebitda", 0),
                tr.GetFieldData("t3320OutBlock1", "evebitda", 0),
                tr.GetFieldData("t3320OutBlock1", "par", 0),
                tr.GetFieldData("t3320OutBlock1", "sps", 0),
                tr.GetFieldData("t3320OutBlock1", "cps", 0),
                tr.GetFieldData("t3320OutBlock1", "bps", 0),
                tr.GetFieldData("t3320OutBlock1", "t_per", 0),
                tr.GetFieldData("t3320OutBlock1", "t_eps", 0),
                tr.GetFieldData("t3320OutBlock1", "peg", 0),
                tr.GetFieldData("t3320OutBlock1", "t_peg", 0),
                tr.GetFieldData("t3320OutBlock1", "t_gsym", 0)
            )
                         )
        except Exception as ex:
            Log.write(str(ex))


class T3320Socket(ResponseAssistant):

    def response_process(self, command, tr):
        pass
