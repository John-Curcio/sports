# to put it politely, sportsbookreview.com and espn use slightly different names
# to put it bluntly, sportsbookreview.com is kind of stupid
import pandas as pd


def manual_fix_ml_df(ml_df):
    to_replace, value = zip(*replace_dict.items()) # gets keys and values of dict respectively
    ml_df["FighterName"] = ml_df["FighterName"].replace(to_replace=to_replace, value=value)
    ml_df["OpponentName"] = ml_df["OpponentName"].replace(to_replace=to_replace, value=value)

    ml_df["OpponentName"].iloc[208] = "talita bernardo" # not germaine de randamie
    ml_df["FighterName"].iloc[239] = "dong hyun ma" # not dong hyun kim
    ml_df["FighterName"].iloc[442] = "dong hyun ma" # again, not dong hyun kim
    ml_df["FighterName"].iloc[495] = "mike rodriguez" # not michael rodriguez
    ml_df["OpponentName"].iloc[561] = "claudio silva" # not henrique da silva

    ml_df["Date"].iloc[639] = pd.to_datetime("2018-07-14") # was 2018-07-13
    ml_df["Date"].iloc[657] = pd.to_datetime("2018-07-14") # was 2018-07-15
    ml_df["Date"].iloc[658] = pd.to_datetime("2018-07-14") # also was 2018-07-15
    ml_df["FighterName"].iloc[766] = "rogerio nogueira" # wrong nog
    ml_df["FighterName"].iloc[807] = "ben saunders" # no idea who sultan aliev is

    ml_df["FighterName"].iloc[1007] = "claudio silva" # not henrique da silva. common mistake
    ml_df["OpponentName"].iloc[1091] = "rogerio nogueira" # again, wrong nog
    ml_df["FighterName"].iloc[1436] = "Isabela de Padua".lower() # was veronica macedo
    ml_df["OpponentName"].iloc[1449] = "mallory martin" # was livinha souza

    ml_df["FighterName"].iloc[1738] = "rogerio nogueira" # again, wrong nog
    ml_df["OpponentName"].iloc[2159] = "matheus nicolau" # 
    ml_df["FighterName"].iloc[2505] = "matheus nicolau" # 
    return ml_df

replace_dict = {
    "rick glenn": "ricky glenn",
    "nina ansaroff": "nina nunes",
    "klidson de abreu": "klidson abreu",
    "j.j. aldrich": "jj aldrich",
    "ode osbourne": "ode' osbourne",
    "saparbek safarov": "saparbeg safarov",
    "seung woo choi": "seungwoo choi",
    # "kennedy nzechukwu": "kennedy nzechukwu",
    "marco polo reyes": "polo reyes",
    "dustin stolzfus": "dustin stoltzfus",
    "zarah fairn dos santos": "zarah fairn",
    "joanne calderwood": "joanne wood",
    "julianna pena": "julianna peña",
    "sergey spivak": "serghei spivac",
    "weili zhang": "zhang weili",
    "ronaldo souza": "jacare souza",
    "cheyanne buys": "cheyanne vlismas",
    "jingliang li": "li jingliang",
    "alex munoz": "alexander munoz",
    "jun yong park": "junyong park",
    "montserrat ruiz": "montserrat conejo",
    "antonio rogerio nogueira": "ANTONIO RODRIGO NOGUEIRA".lower(),
    "darko stosic": "DARKO STOŠIĆ".lower(),
    "antonio carlos junior": "antonio carlos jr.",
    "zu anyanwu": "azunna anyanwu",
    "tony martin": "anthony rocco martin",
    "chan-mi jeon": "chanmi jeon",
    "vincent cachero": "vince cachero",
    "pingyuan liu": "liu pingyuan",
    "da un jung": "da-un jung",
    "daniel hooker": "dan hooker",
    "christopher daukaus": "chris daukaus",
    "charles ontiveros": "charlie ontiveros",
    "geoffrey neal": "geoff neal",
    "xiaonan yan": "yan xiaonan",
    "gabriel green": "gabe green",
    "don tale mayes": "don'tale mayes",
    "alex giplin": "alex gilpin",
    "sung bin jo": "jo sungbin",
    "dennis buzukia": "dennis buzukja",
    "jim crute": "jimmy crute",
    "deiveson alcantara": "deiveson figueiredo",
    "lupita godinez": "loopy godinez",
    "yaozong hu": "hu yaozong",
    "claudio henrique da silva": "henrique da silva",
    "marcos rosa": "marcos mariano",
    "bharat khandare": "bharat kandare",
    "khadzhimurat bestaev": "khadzhi bestaev",
    #"sung-bin jo": "jo sungbin",
    "grigory popov": "grigorii popov",
    "mike chiesa": "michael chiesa",
    "cyril gane": "ciryl gane",
    "alonzo menifeld": "alonzo menifield",
    "alexander romanov": "alexandr romanov",
    "mark madsen": "mark o. madsen",
    "heili alateng": "alateng heili",
    "sean o´malley": "sean o'malley",
    "yadong song": "song yadong", 
    "zhu rong": "rong zhu",
    "joseph solecki": "joe solecki",
    "alexey kunchenko": "aleksei kunchenko",
    "johnny munoz jr": "johnny munoz",
    "nathan maness": "nate maness",
    "joseph lowry": "joe lowry",
    "jeffrey molina": "jeff molina",
    "khalil rountree": "khalil rountree jr.",
    "yanan wu": "wu yanan",
    "daniel omielaniczuk": "daniel omielanczuk",
    "paulo borrachinha": "paulo costa",
    "joshua stansbury": "josh stansbury",
    "cj hamilton": "c.j. hamilton",
    "dmitriy sosnovskiy": "dmitry sosnovskiy",
    "sumudaerji sumudaerji": "Su Mudaerji".lower(),
    "ovince st. preux": "ovince saint preux",
    "j.j. okanovich": "jj okanovich",
    "ariene carnelossi": "ariane carnelossi",
    "chibwikem onyenegecha-palmer": "chibwikem onyenegecha",
    "nicholas musoke": "nico musoke",
    "elizeu zaleski": "elizeu zaleski dos santos",
    "lucasz brzeski": "lukasz brzeski",
    "luiz garagorri": "eduardo garagorri",
    "qileng aori": "aori qileng",
    "josh weems": "joshua weems",
    "kenan song": "song kenan",
    "phillip hawes": "phil hawes",
    "j.r. coughran": "marty coughran",
    "francisco figueredo": "francisco figueiredo",
    "kevin ferguson jr": "kevin ferguson jr.",
    "daniel spohn": "dan spohn",
    "rodrigo vargas": "kazula vargas",
    "leo leite": "leonardo leite",
    "jaimee nievera": "jaimelene nievera",
    "eduarda santana": "duda santana",
    "rick palacios": "ricky palacios",
    "rocco martin": "anthony rocco martin",
    "carlos vergara": "cj vergara",
    "will santiago": "will santiago jr.",
    "vinicius castro": "vinicius moreira",
    "yilan sha": "shayilan NUERDANBIEKE".lower(),
    "kalinn williams": "khaos williams",
    "issac villanueva": "ike villanueva",
    "jay perrin": "jason perrin",
    "riley dutro": "rilley dutro",
    "jeremy wells": "jeremiah wells",
    "zhenhong lu": "lu zhenhong", 
    "michael oleksiejczuk": "michal oleksiejczuk",
    "slava borschev": "VIACHESLAV BORSHCHEV".lower(),
    "na liang": "liang na",
    "isabella de padua": "isabela de padua",
    "marcos rosa mariano": "marcos mariano",
    "jin soon son": "jin soo son",
    "melinda fábián": "melinda fabian",
    "daniel lacerda": "daniel da silva",
    "salim touhari": "salim touahri",
    "igor poterya": "Ihor Potieria".lower(),
    "ciao borralho": "CAIO BORRALHO".lower(),
    "alexey oleinik": "aleksei oleinik",
    "oleksiy oliynyk": "aleksei oleinik",
    "kai kamaka": "kai kamaka iii",
}
