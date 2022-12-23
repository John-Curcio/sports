NAME_REPLACE_DICT = {
    "julianna peña": "julianna pena",
    "marco polo reyes": "polo reyes",
    "brad scott": "bradley scott",
    "nicholas musoke": "nico musoke",
    "paddy holohan": "patrick holohan",
    "alatengheili": "alateng heili",
    "ode osbourne": "ode' osbourne",
    "zhang tiequan": "tiequan zhang",
    "aleksandra albu": "alexandra albu",
    "alvaro herrera mendoza": "alvaro herrera",
    "sumudaerji": "su mudaerji",
    "mark madsen": "mark o. madsen",
    "pingyuan liu": "liu pingyuan",
    "robert mcdaniel": "bubba mcdaniel",
    "aoriqileng": "aori qileng",
    "robert sanchez": "roberto sanchez",
    "patrick smith": "patrick trey smith",
    "aleksandra albu": "alexandra albu",
    "jiří procházka": "jiri prochazka",
}

# 2561001/bruno-carvalho didn't fight 2488370/eiji-mitsuoka on Jul 16, 2011
# that was a different bruno carvalho, who is already in the dataset
MANUAL_ESPN_DROP_PAIR = ("2561001", "2488370")

MANUAL_ESPN_OVERWRITE_MAP = {
    # These espn IDs correspond to the same guy - they have to be merged
    "2583704": "2613376",
    # "2583704/luis-ramos": "2613376/luis-ramos",
}

MANUAL_BFO_OVERWRITE_MAP = {
    # certain bestfightodds.com fighter histories are split btw multiple pages
    '/fighters/Shintaro-Ishiwatar-1151': '/fighters/Shintaro-Ishiwatari-7509',
    '/fighters/Paddy-Holohan-2786': '/fighters/Patrick-Holohan-4991',
    '/fighters/Robert-McDaniel-4064': '/fighters/Bubba-McDaniel-744',
    '/fighters/Nicholas-Musoke-4199': '/fighters/Nico-Musoke-2144',
    '/fighters/Marco-Polo-Reyes-6679': '/fighters/Polo-Reyes-5991',
    '/fighters/Pingyuan-Liu-7732': '/fighters/Liu-Pingyuan-8739',
    '/fighters/Luis-Luna-7785': '/fighters/Anselmo-Luis-Luna-Jr-4330',
    '/fighters/Jung-Bu-Kyung-670': '/fighters/Bukyung-Jung-445',
    '/fighters/Brianna-van-Buren-4076': '/fighters/Brianna-Fortino-13884',
    '/fighters/J-J-Ambrose-12683': '/fighters/J-J-Ambrose-459',
    '/fighters/Anthony-Waldburger-1564': '/fighters/T-J-Waldburger-2156',
    '/fighters/Jadamba-Narantungalag-2028': '/fighters/Narantungalag-Jadambaa-6335', 
#     '/fighters/Narantungalag-Jadambaa-6335': '/fighters/Jadamba-Narantungalag-2028',
    '/fighters/Raquel-Paaluhi-2813': '/fighters/Raquel-Pa-aluhi-5257',
    '/fighters/Rodrigo-Cavalheiro-Correia-5516': '/fighters/Rodrigo-Cavalheiro-4743',
    '/fighters/Jesse-Miele-5797': '/fighters/Jessy-Miele-8855',
    '/fighters/Jp-Buys-12275': '/fighters/J-P-Buys-7455',
    '/fighters/Levy-Marroquin-9617': '/fighters/Levy-Saul-Marroquin-7713',
    '/fighters/Guilherme-Faria-8090': '/fighters/Guillerme-Faria-12163',
    '/fighters/Gabriel-Green-6587': '/fighters/Gabe-Green-10506',
    '/fighters/Philip-Rowe-9379': '/fighters/Phil-Rowe-9898',
    '/fighters/Phillip-Rowe-11319': '/fighters/Phil-Rowe-9898',
    '/fighters/Aleksandra-Albu-5539': '/fighters/Alexandra-Albu-7261',
    '/fighters/Bazigit-Ataev-8579': '/fighters/Bozigit-Ataev-9050',
    '/fighters/Khalil-Rountree-Jr-11552': '/fighters/Khalil-Rountree-4935',
    '/fighters/Khalil-Rountree-Jr-13118': '/fighters/Khalil-Rountree-4935',
    '/fighters/Sumudaerji-Sumudaerji-8746': '/fighters/Su-Mudaerji-9345',
}

MANUAL_UFC_ESPN_MAP = {
    # chris brennan
    "http://ufcstats.com/fighter-details/b19fc66613dc75b9": "2500426",
    # courtney turner
    "http://ufcstats.com/fighter-details/56f4b81ec4db61af": "2951489",
    # patrick trey smith
    "http://ufcstats.com/fighter-details/46c8ec317aff28ac": "2335742",
    # ray wizard
    "http://ufcstats.com/fighter-details/ea0ad155451ed1f5": "2951510",
    # karine silva
    'http://ufcstats.com/fighter-details/9d62c2d8ee151f08': '3309918',
    # rinat fakhretdinov
    'http://ufcstats.com/fighter-details/8f765fd5775a8873': '4712980',
    # jason guida
    'http://ufcstats.com/fighter-details/ce25b4ed82b1811b': '2354107',
    # andrew martinez
    'http://ufcstats.com/fighter-details/f8c2aba4815876b5': '3162579',
    # maheshate
    'http://ufcstats.com/fighter-details/8c1ca54b5089d199': '4895360',
    # kyle driscoll
    'http://ufcstats.com/fighter-details/e5e148d4363deff8': '4246527',
    # JR/Marty Coughran
    'http://ufcstats.com/fighter-details/8112c9a23dccc759': '4354427',
    # Drew dimanlig
    'http://ufcstats.com/fighter-details/cc2ad11b1f9d818b': '2559902',
    # damon jackson
    'http://ufcstats.com/fighter-details/29af297d9f1de0f8': '3099187',
    # dan argueta
    'http://ufcstats.com/fighter-details/e4ba58725825412d': '4815973',
    # askar mozharov
    'http://ufcstats.com/fighter-details/e92901944ce91909': '4217396',
    # mabelly lima
    'http://ufcstats.com/fighter-details/6135fd9665fbb74e': '4372190',
    # mario rivera
    'http://ufcstats.com/fighter-details/0f7210aa8d61af8d': '2951376',
    # dan molina
    'http://ufcstats.com/fighter-details/606136dee6f6ecea': '2556050',
    # jeremy freitag
    'http://ufcstats.com/fighter-details/a47e9ec288c91067': '2556758',
    # naoki matsushita
    'http://ufcstats.com/fighter-details/990060b2a68a7b82': '2553054',
    # luciano azevedo
    'http://ufcstats.com/fighter-details/9bcfb40dbcd50568': '2381679',
    # thiago moises
    'http://ufcstats.com/fighter-details/d945aae53e3e54e6': '3955778',
    # gleidson cutis
    'http://ufcstats.com/fighter-details/44a94bbde42246e4': '4372295',
    # dayana silva
    'http://ufcstats.com/fighter-details/b19aecbfbb5508cc': '3971629',
    # gisele moreira
    'http://ufcstats.com/fighter-details/6a125ba3ec37e27e': '4030631',
    # patrick murphy
    'http://ufcstats.com/fighter-details/eca7e064746c161a': '3039036',
    # josh mcdonald
    'http://ufcstats.com/fighter-details/b507a76087e3ed9f': '2527951',
    # rafael de real
    'http://ufcstats.com/fighter-details/e82b2adcaeff71ec': '2500780',
    # trevor harris
    'http://ufcstats.com/fighter-details/0e98b05d3cf6d271': '2969478',
    # kenny ento
    'http://ufcstats.com/fighter-details/daf9be103c1edbbd': '2965044',
}

MANUAL_ESPN_BFO_MAP = {
    # 3041602/brianna-fortino
    '3041602': '/fighters/Brianna-Fortino-13884',
    # /3153355/uyran-carlos
    '3153355': '/fighters/Uyran-Carlos-11754',
    # 3146349/carlos-leal
    '3146349': '/fighters/Carlos-Leal-Miranda-7744',
    # 3153355/uyran-carlos
    '3153355': '/fighters/Uyran-Carlos-11754',
    # 4916590/diego-dias
    '4916590': '/fighters/Diego-Dias-11750',
    # 2431314/jacare-souza
    '2431314': '/fighters/Ronaldo-Souza-725',
    # 2555633/jj-ambrose
#     '2555633': '/fighters/J-J-Ambrose-459',
    # /2558487/tony-johnson-jr
    '2558487': '/fighters/Tony-Johnson-918',
    # 2504175/zachary-micklewright
    '2504175': '/fighters/Zach-Micklewright-1651',
    # rodrigo de lima
    '3110330': '/fighters/Rodrigo-Goiana-de-Lima-4992',
    # /4030644/marcelo-rojo
    '4030644': '/fighters/Marcelo-Rojo-7706',
    # 3083639/mike-erosa
    '3083639': '/fighters/Mikey-Erosa-7707',
#     '4335927/levy-saul-marroquin-salazar'
    '4335927': '/fighters/Levy-Saul-Marroquin-7713',
    '4063869': '/fighters/John-Castaneda-7396',
    
    '4423264': 'fighters/Tofiq-Musaev-9177',
    '4306125': '/fighters/Gabe-Green-10506',
    '4914568': '/fighters/Pete-Rodrigue-13104',
    '3091146': '/fighters/Toninho-Gavinho-11224',
    '3074493': '/fighters/Alexandra-Albu-7261',
    '2509773': '/fighters/Shintaro-Ishiwatari-7509',
    '2500906': '/fighters/Bozigit-Ataev-9050',
    '4405109': '/fighters/Su-Mudaerji-9345',
}