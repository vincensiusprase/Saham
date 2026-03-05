# ============================================================
# BACKTESTING ENGINE — CANDLESTICK PATTERN STRATEGY
# Modal Awal  : Rp 2.000.000
# Strategi    : Long Only | Full Compounding
# Periode     : 2 Tahun ke belakang (harian)
# Exit Rules  : TP (2R) | SL (Low sinyal) | Time Exit (10 hari)
# ============================================================

import yfinance as yf
import pandas as pd
import numpy as np
import ta
import gspread
from gspread_dataframe import set_with_dataframe
from datetime import datetime, timezone, timedelta, date
import warnings
import json
import os
import time

warnings.filterwarnings('ignore')

# ============================================================
# KONFIGURASI
# ============================================================
SPREADSHEET_ID   = "1Fnfotp0hpZxNqGEko3AP2GJqR5EMimQ-bJGj3AWsrnw"
MODAL_AWAL       = 2_000_000
BACKTEST_YEARS   = 2
MAX_HOLD_DAYS    = 10
RR_TARGET        = 2.0      # Risk:Reward 1:2
LOT_SIZE         = 100      # 1 lot = 100 lembar saham (IDX)

# ============================================================
# DAFTAR SAHAM (semua sektor)
# ============================================================
ALL_TICKERS = [
    #IDXINDUS
        "ASII.JK", "UNTR.JK", "PIPA.JK", "BNBR.JK", "HEXA.JK", "IMPC.JK", "MHKI.JK", "LABA.JK", "SMIL.JK", "NTBK.JK", 
        "PADA.JK", "NAIK.JK", "TOTO.JK", "BHIT.JK", "MARK.JK", "INDX.JK", "PTMP.JK", "ARNA.JK", "KOBX.JK", "ASGR.JK", 
        "GPSO.JK", "SINI.JK", "HOPE.JK", "FOLK.JK", "KUAS.JK", "KBLI.JK", "SPTO.JK", "IKAI.JK", "SKRN.JK", "MUTU.JK", 
        "CTTH.JK", "CAKK.JK", "CCSI.JK", "MLIA.JK", "JTPE.JK", "BLUE.JK", "CRSN.JK", "DYAN.JK", "LION.JK", "MDRN.JK", 
        "VOKS.JK", "SCCO.JK", "KBLM.JK", "JECC.JK", "INTA.JK", "BINO.JK", "VISI.JK", "ARKA.JK", "SOSS.JK", "IKBI.JK", 
        "AMIN.JK", "ICON.JK", "KONI.JK", "AMFG.JK", "IBFN.JK", "ZBRA.JK", "TIRA.JK", "KIAS.JK", "APII.JK", "HYGN.JK", 
        "KOIN.JK", "MFMI.JK", "TRIL.JK"
    ,
    #IDXNONCYC
        "UNVR.JK", "INDF.JK", "ICBP.JK", "GGRM.JK", "AALI.JK", "JPFA.JK", "MYOR.JK", "CPIN.JK", "RLCO.JK", "LSIP.JK", 
        "HMSD.JK", "AMRT.JK", "GZCO.JK", "MLPL.JK", "JARR.JK", "BWPT.JK", "ULTJ.JK", "FORE.JK", "TAPG.JK", "CPRO.JK", 
        "SIMP.JK", "CLEO.JK", "WIIM.JK", "MPPA.JK", "HOKI.JK", "COCO.JK", "DSNG.JK", "IKAN.JK", "SMAR.JK", "BRRC.JK", 
        "PTPS.JK", "SSMS.JK", "GOOD.JK", "PGUN.JK", "SGRO.JK", "BEEF.JK", "BTEK.JK", "MAIN.JK", "MIDI.JK", "AYAM.JK", 
        "WMUU.JK", "DEWI.JK", "ASHA.JK", "CMRY.JK", "AISA.JK", "CAMP.JK", "OILS.JK", "ANJT.JK", "TBLA.JK", "ROTI.JK", 
        "NASI.JK", "ITIC.JK", "DSFI.JK", "ISEA.JK", "JAWA.JK", "NSSS.JK", "CSRA.JK", "STAA.JK", "KEJU.JK", "UNSP.JK", 
        "MRAT.JK", "DPUM.JK", "PSDN.JK", "STRK.JK", "WAPO.JK", "MBTO.JK", "GULA.JK", "LAPD.JK", "RANC.JK", "NEST.JK", 
        "TAYS.JK", "CEKA.JK", "ADES.JK", "KINO.JK", "BISI.JK", "ANDI.JK", "PMMP.JK", "DMND.JK", "UCID.JK", "YUPI.JK", 
        "BEER.JK", "PSGO.JK", "BUDI.JK", "SKBM.JK", "MLBI.JK", "TLDN.JK", "FOOD.JK", "MKTR.JK", "SKLT.JK", "MAXI.JK", 
        "STTP.JK", "PNGO.JK", "PCAR.JK", "GUNA.JK", "EPMT.JK", "DLTA.JK", "HERO.JK", "TGUK.JK", "VICI.JK", "MSJA.JK", 
        "SDPC.JK", "MGRO.JK", "FISH.JK", "BOBA.JK", "KMDS.JK", "TCID.JK", "BUAH.JK", "TGKA.JK", "WINE.JK", "ENZO.JK",
        "CBUT.JK", "WMPP.JK", "IPPE.JK", "DAYA.JK", "SIPD.JK", "CRAB.JK", "FAPA.JK", "AGAR.JK", "TRGU.JK", "ALTO.JK", 
        "MAGP.JK", "GOLL.JK", "WICO.JK"
    ,
   #IDXFINANCE
    "BBCA.JK", "BBRI.JK", "BMRI.JK", "BBNI.JK", "BRIS.JK", "SUPA.JK", "COIN.JK", "BBTN.JK", "ARTO.JK", "BBYB.JK", "BNGA.JK", "BBKP.JK",
    "BTPS.JK", "BJTM.JK", "SRTG.JK", "PNLF.JK", "PADI.JK", "AGRO.JK", "NISP.JK", "INPC.JK", "BJBR.JK", "BBHI.JK", "BFIN.JK", "BDMN.JK",
    "BABP.JK", "PNBS.JK", "BGTG.JK", "AHAP.JK", "BANK.JK", "BACA.JK", "BNLI.JK", "BNII.JK", "BCAP.JK", "PNBN.JK", "MEGA.JK", "BVIC.JK",
    "ADMF.JK", "DNAR.JK", "MAYA.JK", "CFIN.JK", "BTPN.JK", "BSIM.JK", "BEKS.JK", "TUGU.JK", "PEGE.JK", "NOBU.JK", "PALM.JK", "BNBA.JK",
    "LPPS.JK", "AGRS.JK", "DNET.JK", "AMAR.JK", "GSMF.JK", "JMAS.JK", "TRIM.JK", "MCOR.JK", "PNIN.JK", "SMMA.JK", "PANS.JK", "BKSW.JK",
    "VINS.JK", "BCIC.JK", "BINA.JK", "WOMF.JK", "LPGI.JK", "LIFE.JK", "VTNY.JK", "VICO.JK", "STAR.JK", "YOII.JK", "FUJI.JK", "MTWI.JK",
    "POLA.JK", "BBSI.JK", "ASJT.JK", "SDRA.JK", "BMAS.JK", "AMAG.JK", "ASMI.JK", "HDFA.JK", "VRNA.JK", "AMOR.JK", "APIC.JK", "MREI.JK",
    "ASDM.JK", "TIFA.JK", "BHIT.JK", "ASRM.JK", "RELI.JK", "NICK.JK", "TRUS.JK", "ASBI.JK", "DEFI.JK", "BBLD.JK", "BBMD.JK", "MASB.JK",
    "BPFI.JK", "YULE.JK", "BPII.JK", "POOL.JK", "BSWD.JK", "SFAN.JK", "ABDA.JK", "OCAP.JK", "PLAS.JK"
    ,
    #IDXCYCLIC
    "MNCN.JK", "SCMA.JK", "LPPF.JK", "MINA.JK", "BUVA.JK", "ACES.JK", "ERAA.JK", "HRTA.JK", "FUTR.JK", "MAPI.JK", "AUTO.JK", "GJTL.JK",
    "FAST.JK", "VKTR.JK", "DOOH.JK", "BMTR.JK", "MPMX.JK", "FILM.JK", "RALS.JK", "KPIG.JK", "MAPA.JK", "SLIS.JK", "ZATA.JK", "SMSM.JK",
    "JGLE.JK", "ASLC.JK", "IMAS.JK", "MERI.JK", "NETV.JK", "KAQI.JK", "CNMA.JK", "MSIN.JK", "WOOD.JK", "BELL.JK", "PSKT.JK", "VIVA.JK",
    "MSKY.JK", "BABY.JK", "YELO.JK", "IPTV.JK", "TMPO.JK", "JIHD.JK", "DOSS.JK", "PMUI.JK", "SRIL.JK", "ERAL.JK", "DRMA.JK", "GOLF.JK",
    "ESTA.JK", "DFAM.JK", "PBRX.JK", "PZZA.JK", "BAIK.JK", "MDIA.JK", "CARS.JK", "ABBA.JK", "GEMA.JK", "PART.JK", "SWID.JK", "EAST.JK",
    "MARI.JK", "UNTD.JK", "KDTN.JK", "ACRO.JK", "ERTX.JK", "VERN.JK", "BOLA.JK", "KOTA.JK", "MDIY.JK", "FITT.JK", "TOOL.JK", "INDR.JK",
    "LIVE.JK", "PJAA.JK", "RAAM.JK", "INOV.JK", "CINT.JK", "KICI.JK", "FORU.JK", "ECII.JK", "GRPH.JK", "PANR.JK", "NATO.JK", "LPIN.JK",
    "CSMI.JK", "TRIS.JK", "UFOE.JK", "BOGA.JK", "SSTM.JK", "MGNA.JK", "DEPO.JK", "ESTI.JK", "POLU.JK", "SOTS.JK", "INDS.JK", "RAFI.JK",
    "BAYU.JK", "TOYS.JK", "GDYR.JK", "SONA.JK", "MAPB.JK", "PGLI.JK", "BAUT.JK", "GWSA.JK", "HRME.JK", "BIKE.JK", "DIGI.JK", "JSPT.JK",
    "MICE.JK", "LMPI.JK", "CSAP.JK", "BIMA.JK", "POLY.JK", "SHID.JK", "PTSP.JK", "SBAT.JK", "SCNP.JK", "RICY.JK", "BRAM.JK", "ENAK.JK",
    "PMJS.JK", "SNLK.JK", "TELE.JK", "BATA.JK", "ARGO.JK", "ZONE.JK", "BOLT.JK", "PNSE.JK", "DUCK.JK", "TYRE.JK", "CLAY.JK", "ARTA.JK",
    "IIKP.JK", "PDES.JK", "CBMF.JK", "BLTZ.JK", "HOME.JK", "TFCO.JK", "GLOB.JK", "AKKU.JK", "MYTX.JK", "CNTX.JK", "UNIT.JK", "TRIO.JK",
    "NUSA.JK", "HOTL.JK", "MABA.JK"
    ,
    #IDXTECHNO
    "GOTO.JK", "WIFI.JK", "EMTK.JK", "BUKA.JK", "WIRG.JK", "DCII.JK", "IOTF.JK", "MTDL.JK", "ELIT.JK", "MLPT.JK", "DMMX.JK",
    "TOSK.JK", "JATI.JK", "KIOS.JK", "IRSX.JK", "UVCR.JK", "TRON.JK", "KREN.JK", "CYBR.JK", "LUCK.JK", "PTSN.JK", "HDIT.JK",
    "EDGE.JK", "DIVA.JK", "TFAS.JK", "ZYRX.JK", "MSTI.JK", "MCAS.JK", "MPIX.JK", "BELI.JK", "AXIO.JK", "AWAN.JK", "AREA.JK",
    "NFCX.JK", "ATIC.JK", "TECH.JK", "GLVA.JK", "ENVY.JK", "LMAS.JK", "SKYB.JK"
    ,
    #IDXBASIC
    "ANTM.JK", "BRMS.JK", "SMGR.JK", "BRPT.JK", "INTP.JK", "EMAS.JK", "MDKA.JK", "INCO.JK", "TINS.JK", "ARCI.JK", "TPIA.JK",
    "MBMA.JK", "INKP.JK", "PSAB.JK", "NCKL.JK", "AMMN.JK", "ESSA.JK", "TKIM.JK", "KRAS.JK", "DKFT.JK", "NICL.JK", "FPNI.JK",
    "WSBP.JK", "SMBR.JK", "WTON.JK", "SMGA.JK", "AGII.JK", "AVIA.JK", "NIKL.JK", "SOLA.JK", "ISSP.JK", "MINE.JK", "DAAZ.JK",
    "OKAS.JK", "OPMS.JK", "BAJA.JK", "NICE.JK", "CHEM.JK", "ZINC.JK", "PPRI.JK", "AYLS.JK", "SRSN.JK", "EKAD.JK", "PBID.JK",
    "PICO.JK", "ESIP.JK", "CITA.JK", "MOLI.JK", "GDST.JK", "SULI.JK", "TIRT.JK", "MDKI.JK", "ADMG.JK", "SPMA.JK", "SMLE.JK",
    "CLPI.JK", "ASPR.JK", "NPGF.JK", "BLES.JK", "BATR.JK", "DGWG.JK", "GGRP.JK", "FWCT.JK", "TBMS.JK", "PDPP.JK", "LTLS.JK",
    "SAMF.JK", "BMSR.JK", "BEBS.JK", "SBMA.JK", "PTMR.JK", "IPOL.JK", "UNIC.JK", "OBMD.JK", "KAYU.JK", "SMCB.JK", "IGAR.JK",
    "INCI.JK", "INCF.JK", "EPAC.JK", "INAI.JK", "ALDO.JK", "HKMU.JK", "SQMI.JK", "SMKL.JK", "IFII.JK", "IFSH.JK", "PURE.JK",
    "SWAT.JK", "BTON.JK", "TALF.JK", "KDSI.JK", "INRU.JK", "CMNT.JK", "INTD.JK", "ALKA.JK", "KMTR.JK", "CTBN.JK", "YPAS.JK",
    "KKES.JK", "AKPI.JK", "DPNS.JK", "APLI.JK", "TRST.JK", "BRNA.JK", "LMSH.JK","ALMI.JK","FASW.JK","ETWA.JK","TDPM.JK","SIMA.JK","KBRI.JK"
    ,
    #IDXENERGY
    "ADRO.JK", "BUMI.JK", "PGAS.JK", "PTBA.JK", "ITMG.JK", "DEWA.JK", "CUAN.JK", "HRUM.JK", "PTRO.JK", "RAJA.JK", "MEDC.JK", "ADMR.JK",
    "HUMI.JK", "ENRG.JK", "BULL.JK", "TOBA.JK", "AADI.JK", "RATU.JK", "CBRE.JK", "INDY.JK", "AKRA.JK", "ELSA.JK", "GTSI.JK", "BIPI.JK",
    "COAL.JK", "BSSR.JK", "LEAD.JK", "APEX.JK", "TEBE.JK", "ATLA.JK", "SOCI.JK", "FIRE.JK", "PSAT.JK", "GEMS.JK", "DOID.JK", "DSSA.JK",
    "SGER.JK", "IATA.JK", "BBRM.JK", "BYAN.JK", "ABMM.JK", "TPMA.JK", "MAHA.JK", "BOAT.JK", "KKGI.JK", "MBSS.JK", "WOWS.JK", "CGAS.JK",
    "RMKE.JK", "WINS.JK", "MTFN.JK", "MBAP.JK", "UNIQ.JK", "RMKO.JK", "SMMT.JK", "SICO.JK", "BSML.JK", "PSSI.JK", "DWGL.JK", "TAMU.JK",
    "ALII.JK", "ITMA.JK", "RUIS.JK", "CNKO.JK", "TCPI.JK", "HILL.JK", "BOSS.JK", "PKPK.JK", "MYOH.JK", "SEMA.JK", "ARII.JK", "GTBO.JK",
    "MCOL.JK", "RGAS.JK", "SHIP.JK", "BESS.JK", "RIGS.JK", "JSKY.JK", "KOPI.JK", "PTIS.JK", "CANI.JK", "ARTI.JK", "INPS.JK", "MKAP.JK",
    "AIMS.JK", "HITS.JK", "SUNI.JK", "TRAM.JK", "SURE.JK", "SMRU.JK", "SUGI.JK"
    ,
    #IDXHEALTH
    "KLBF.JK", "SIDO.JK", "KAEF.JK", "PYFA.JK", "MIKA.JK", "DKHH.JK", "SILO.JK", "HEAL.JK", "TSPC.JK", "INAF.JK", "CHEK.JK", "IRRA.JK", "SAME.JK", "MEDS.JK", "PRDA.JK", "MDLA.JK", "SURI.JK", "PRIM.JK", "HALO.JK", "OBAT.JK", "CARE.JK",
    "MERK.JK", "DGNS.JK", "SOHO.JK", "BMHS.JK", "PEHA.JK", "SRAJ.JK", "MMIX.JK", "DVLA.JK", "OMED.JK", "PEVE.JK", "LABS.JK", "RSCH.JK", "MTMH.JK", "IKPM.JK", "PRAY.JK", "SCPI.JK", "RSGK.JK"
    ,
    #IDXINFRA
    "TLKM.JK", "CDIA.JK", "ADHI.JK", "JSMR.JK", "WIKA.JK", "PTPP.JK", "INET.JK", "WSKT.JK", "BREN.JK", "PGEO.JK", "EXCL.JK", "ISAT.JK", "TOWR.JK", "SSIA.JK", "DATA.JK", "OASA.JK", "PPRE.JK", "TBIG.JK", "POWR.JK", "NRCA.JK", "WEGE.JK", "TOTL.JK",
    "KETR.JK", "IPCC.JK", "KOKA.JK", "KBLV.JK", "MTEL.JK", "CENT.JK", "KRYA.JK", "GMFI.JK", "JAST.JK", "KEEN.JK", "JKON.JK", "ACST.JK", "ASLI.JK", "PBSA.JK", "IPCM.JK", "MORA.JK", "ARKO.JK", "MPOW.JK", "CMNP.JK", "LINK.JK", "HGII.JK", "DGIK.JK", "BDKR.JK",
    "META.JK", "KARW.JK", "CASS.JK", "BUKK.JK", "TGRA.JK", "GOLD.JK", "BALI.JK", "PTDU.JK", "IDPR.JK", "PORT.JK", "TOPS.JK", "HADE.JK", "TAMA.JK", "BTEL.JK", "GHON.JK", "SUPR.JK", "MTPS.JK", "RONY.JK", "IBST.JK", "LCKM.JK", "PTPW.JK", "MTRA.JK"
    ,
    #IDXPROPERT
    "CTRA.JK", "BSDE.JK", "PWON.JK", "SMRA.JK", "KLJA.JK", "PANI.JK", "BKSL.JK", "DADA.JK", "CBDK.JK", "DMAS.JK", "ASRI.JK", "LPKR.JK", "BSBK.JK", "REAL.JK", "ELTY.JK", "APLN.JK", "TRUE.JK", "TRIN.JK", "UANG.JK", "CSIS.JK", "DILD.JK", "KOCI.JK", "BEST.JK",
    "LAND.JK", "DUTI.JK", "EMDE.JK", "LPLI.JK", "GRIA.JK", "VAST.JK", "BAPI.JK", "MTLA.JK", "SAGE.JK", "BBSS.JK", "HOMI.JK", "PUDP.JK", "RBMS.JK", "URBN.JK", "TARA.JK", "CBPE.JK", "MPRO.JK", "RODA.JK", "SATU.JK", "NASA.JK", "FMII.JK", "BKDP.JK", "GMTD.JK",
    "PPRO.JK", "BAPA.JK", "PAMG.JK", "MMLP.JK", "PURI.JK", "GPRA.JK", "LPCK.JK", "MDLN.JK", "BCIP.JK", "ADCP.JK", "CITY.JK", "RISE.JK", "WINR.JK", "JRPT.JK", "AMAN.JK", "SMDM.JK", "INDO.JK", "ATAP.JK", "ASPI.JK", "KSIX.JK", "KBAG.JK", "NZIA.JK",
    "NIRO.JK", "DART.JK", "BIPP.JK", "PLIN.JK", "RDTX.JK", "ROCK.JK", "MKPI.JK", "INPP.JK", "MTSM.JK", "POLL.JK", "POLI.JK", "OMRE.JK", "GAMA.JK", "POSA.JK", "BIKA.JK", "CPRI.JK", "ARMY.JK", "COWL.JK", "RIMO.JK", "LCGP.JK"
    ,
    #IDXTRANS
    "PJHB.JK", "GIAA.JK", "SMDR.JK", "BIRD.JK", "BLOG.JK", "IMJS.JK", "ASSA.JK", "TMAS.JK", "LAJU.JK", "HAIS.JK", "KLAS.JK", "MITI.JK", "JAYA.JK", "NELY.JK", "WEHA.JK", "TNCA.JK", "CMPP.JK", "MPXL.JK", "KJEN.JK", "SDMU.JK", "TRUK.JK", "PURA.JK", "HATM.JK",
    "TAXI.JK", "ELPI.JK", "AKSI.JK", "GTRA.JK", "TRJA.JK", "MIRA.JK", "BLTA.JK", "SAPX.JK", "SAFE.JK", "LRNA.JK", "DEAL.JK", "BPTR.JK", "HELI.JK"
    ]

# ============================================================
# GOOGLE SHEETS CONNECTION
# ============================================================
def connect_gsheet(sheet_name):
    try:
        creds_json = os.environ.get("GCP_SA_KEY")
        if not creds_json:
            print("❌ GCP_SA_KEY tidak ditemukan.")
            return None
        creds_dict = json.loads(creds_json)
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        from google.oauth2.service_account import Credentials
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        gc    = gspread.authorize(creds)
        sh    = gc.open_by_key(SPREADSHEET_ID)
        try:
            ws = sh.worksheet(sheet_name)
        except:
            ws = sh.add_worksheet(title=sheet_name, rows="2000", cols="20")
        return ws
    except Exception as e:
        print(f"❌ GSheet Error: {e}")
        return None

# ============================================================
# HELPER FUNCTIONS
# ============================================================
def is_near(p1, p2, pct=0.001):
    return abs(p1 - p2) / max(p1, p2) <= pct

def close_near_high(row, thr=0.2):
    l = row['High'] - row['Low']
    return (row['High'] - max(row['Open'], row['Close'])) <= thr * l if l > 0 else True

def close_near_low(row, thr=0.2):
    l = row['High'] - row['Low']
    return (min(row['Open'], row['Close']) - row['Low']) <= thr * l if l > 0 else True

def get_pattern_score(base_prob, tier, confluence):
    tier_mult        = {1: 1.00, 2: 1.15, 3: 1.30}.get(tier, 1.00)
    confluence_bonus = 1 + (confluence * 0.05)
    return round(min(95, base_prob * 100 * tier_mult * confluence_bonus), 1)

def get_tier_stars(tier):
    return {1:'⭐', 2:'⭐⭐', 3:'⭐⭐⭐'}.get(tier, '')

# ============================================================
# INDICATOR CALCULATION
# ============================================================
def calc_indicators(df):
    """Hitung semua indikator teknikal. Return df yang sudah lengkap."""
    for p in [10, 20, 50, 100, 200]:
        df[f'SMA_{p}'] = ta.trend.sma_indicator(df['Close'], window=p)
        df[f'EMA_{p}'] = ta.trend.ema_indicator(df['Close'], window=p)

    ichi = ta.trend.IchimokuIndicator(df['High'], df['Low'], 9, 26, 52)
    df['ISA'] = ichi.ichimoku_a()
    df['ISB'] = ichi.ichimoku_b()
    df['ITS'] = ichi.ichimoku_conversion_line()
    df['IKS'] = ichi.ichimoku_base_line()

    df['RSI']    = ta.momentum.rsi(df['Close'], 14)
    stoch        = ta.momentum.StochasticOscillator(df['High'], df['Low'], df['Close'], 14, 3)
    df['STOCH_K'] = stoch.stoch()
    df['STOCH_D'] = stoch.stoch_signal()
    df['CCI']    = ta.trend.cci(df['High'], df['Low'], df['Close'], 20)
    adx          = ta.trend.ADXIndicator(df['High'], df['Low'], df['Close'], 14)
    df['ADX']    = adx.adx()
    df['+DI']    = adx.adx_pos()
    df['-DI']    = adx.adx_neg()
    df['AO']     = ta.momentum.awesome_oscillator(df['High'], df['Low'], 5, 34)
    df['MOM']    = df['Close'].diff(10)
    macd         = ta.trend.MACD(df['Close'], 26, 12, 9)
    df['MACD']        = macd.macd()
    df['MACD_SIGNAL'] = macd.macd_signal()
    srsi         = ta.momentum.StochRSIIndicator(df['Close'], 14, 3, 3)
    df['SRSI_K'] = srsi.stochrsi_k() * 100
    df['SRSI_D'] = srsi.stochrsi_d() * 100
    df['WILLR']  = ta.momentum.williams_r(df['High'], df['Low'], df['Close'], 14)
    df['EMA_13'] = ta.trend.ema_indicator(df['Close'], 13)
    df['BULLP']  = df['High'] - df['EMA_13']
    df['BEARP']  = df['Low']  - df['EMA_13']
    df['UO']     = ta.momentum.ultimate_oscillator(df['High'], df['Low'], df['Close'], 7, 14, 28)
    df['VOL_SMA_20'] = df['Volume'].rolling(20).mean()
    df['ATR']    = ta.volatility.AverageTrueRange(df['High'], df['Low'], df['Close'], 14).average_true_range()
    df['ATR_SMA20'] = df['ATR'].rolling(20).mean()
    return df

# ============================================================
# SIGNAL GENERATOR (per baris / tanggal)
# ============================================================
def generate_signal(df, idx):
    """
    Evaluasi sinyal pada posisi idx (hari ini = idx).
    Return dict sinyal atau None.
    idx harus >= 5 (butuh 5 baris sebelumnya untuk pola island dll)
    """
    try:
        if idx < 5:
            return None

        # Slice 5 candle terakhir
        d1 = df.iloc[idx-4]   # 4 hari lalu
        d2 = df.iloc[idx-3]
        d3 = df.iloc[idx-2]
        d4 = df.iloc[idx-1]
        d5 = df.iloc[idx]     # hari ini (sinyal)

        # alias: day1=2 hari lalu, day2=kemarin, day3=hari ini
        day1, day2, day3 = d3, d4, d5

        # ---------- Pre-computed metrics ----------
        body1  = abs(day1['Close']-day1['Open']);  range1 = day1['High']-day1['Low']
        body2  = abs(day2['Close']-day2['Open']);  range2 = day2['High']-day2['Low']
        body3  = abs(day3['Close']-day3['Open']);  range3 = day3['High']-day3['Low']

        us3 = day3['High'] - max(day3['Open'], day3['Close'])
        ls3 = min(day3['Open'], day3['Close']) - day3['Low']

        bull1 = day1['Close'] > day1['Open'];  bear1 = not bull1
        bull2 = day2['Close'] > day2['Open'];  bear2 = not bull2
        bull3 = day3['Close'] > day3['Open'];  bear3 = not bull3

        atr3 = day3['ATR']
        valid_atr = atr3 > 0

        bt1 = max(day1['Open'], day1['Close']); bb1 = min(day1['Open'], day1['Close'])
        bt2 = max(day2['Open'], day2['Close']); bb2 = min(day2['Open'], day2['Close'])
        bt3 = max(day3['Open'], day3['Close']); bb3 = min(day3['Open'], day3['Close'])

        mid1 = (day1['Open']+day1['Close'])/2
        mid2 = (day2['Open']+day2['Close'])/2

        # ---------- Konteks trend ----------
        is_proper_down = (
            (day3['SMA_20'] < day3['SMA_50']) and
            (day3['SMA_20'] < df['SMA_20'].iloc[idx-5]) and
            (day3['Close']  < day3['SMA_50'])
        )
        is_proper_up = (
            (day3['SMA_20'] > day3['SMA_50']) and
            (day3['SMA_20'] > df['SMA_20'].iloc[idx-5]) and
            (day3['Close']  > day3['SMA_50'])
        )
        is_down_basic = day3['SMA_20'] < day3['SMA_50']
        is_up_basic   = day3['SMA_20'] > day3['SMA_50']

        candle_ok   = (range3 >= atr3 * 0.7) if valid_atr else True
        vol_thrust  = day3['Volume'] > day3['VOL_SMA_20'] * 1.5
        vol_above   = day3['Volume'] > day3['VOL_SMA_20']
        is_volatile = day3['ATR'] > day3['ATR_SMA20'] * 0.75 if valid_atr else True

        ext_os  = day3['RSI'] < 35
        os_ctx  = day3['RSI'] < 45
        macd_b  = day3['MACD'] > day3['MACD_SIGNAL']
        ranging = day3['ADX'] < 20

        rh20 = df['High'].iloc[max(0,idx-19):idx+1].max()
        rl20 = df['Low'].iloc[max(0,idx-19):idx+1].min()
        pr20 = rh20 - rl20
        pp   = (day3['Close']-rl20)/pr20 if pr20 > 0 else 0.5
        near_sup = pp < 0.25

        # ---------- TradingView Score ----------
        sc, cnt = 0, 0
        def add(v):
            nonlocal sc, cnt; sc += v; cnt += 1

        for p in [10,20,50,100,200]:
            if pd.notna(day3[f'SMA_{p}']):
                add(1 if day3[f'SMA_{p}']<day3['Close'] else -1 if day3[f'SMA_{p}']>day3['Close'] else 0)
            if pd.notna(day3[f'EMA_{p}']):
                add(1 if day3[f'EMA_{p}']<day3['Close'] else -1 if day3[f'EMA_{p}']>day3['Close'] else 0)

        if (day3['ISA']>day3['ISB']) and (day3['IKS']>day3['ISA']) and (day3['ITS']>day3['IKS']) and (day3['Close']>day3['ITS']): add(1)
        elif (day3['ISA']<day3['ISB']) and (day3['IKS']<day3['ISA']) and (day3['ITS']<day3['IKS']) and (day3['Close']<day3['ITS']): add(-1)
        else: add(0)

        if day3['RSI']<30 and day3['RSI']>day2['RSI']:    add(1)
        elif day3['RSI']>70 and day3['RSI']<day2['RSI']:  add(-1)
        else: add(0)

        if day3['STOCH_K']<20 and day3['STOCH_D']<20 and day3['STOCH_K']>day3['STOCH_D']:    add(1)
        elif day3['STOCH_K']>80 and day3['STOCH_D']>80 and day3['STOCH_K']<day3['STOCH_D']:  add(-1)
        else: add(0)

        if day3['CCI']<-100 and day3['CCI']>day2['CCI']:   add(1)
        elif day3['CCI']>100 and day3['CCI']<day2['CCI']:  add(-1)
        else: add(0)

        if day3['+DI']>day3['-DI'] and day3['ADX']>20 and day3['ADX']>day2['ADX']:    add(1)
        elif day3['+DI']<day3['-DI'] and day3['ADX']>20 and day3['ADX']>day2['ADX']:  add(-1)
        else: add(0)

        ao_sb = (day3['AO']>0) and (d3['AO']>d4['AO']) and (d5['AO']>d4['AO'])
        ao_cb = (d4['AO']<0) and (d5['AO']>0)
        ao_ss = (day3['AO']<0) and (d3['AO']<d4['AO']) and (d5['AO']<d4['AO'])
        ao_cs = (d4['AO']>0) and (d5['AO']<0)
        if ao_sb or ao_cb: add(1)
        elif ao_ss or ao_cs: add(-1)
        else: add(0)

        if day3['MOM']>day2['MOM']: add(1)
        elif day3['MOM']<day2['MOM']: add(-1)
        else: add(0)

        if day3['MACD']>day3['MACD_SIGNAL']: add(1)
        elif day3['MACD']<day3['MACD_SIGNAL']: add(-1)
        else: add(0)

        tren_naik = day3['EMA_13']>day2['EMA_13']
        if not tren_naik and day3['SRSI_K']<20 and day3['SRSI_D']<20 and day3['SRSI_K']>day3['SRSI_D']: add(1)
        elif tren_naik and day3['SRSI_K']>80 and day3['SRSI_D']>80 and day3['SRSI_K']<day3['SRSI_D']:   add(-1)
        else: add(0)

        if day3['WILLR']<-80 and day3['WILLR']>day2['WILLR']:   add(1)
        elif day3['WILLR']>-20 and day3['WILLR']<day2['WILLR']: add(-1)
        else: add(0)

        if tren_naik and day3['BEARP']<0 and day3['BEARP']>day2['BEARP']:   add(1)
        elif tren_naik and day3['BULLP']>0 and day3['BULLP']<day2['BULLP']: add(-1)
        else: add(0)

        if day3['UO']>70: add(1)
        elif day3['UO']<30: add(-1)
        else: add(0)

        tv_val = sc/cnt if cnt>0 else 0
        if   tv_val <-0.5:  tv_rec = "Penjualan Kuat"
        elif tv_val <-0.1:  tv_rec = "Penjualan"
        elif tv_val <= 0.1: tv_rec = "Netral"
        elif tv_val <= 0.5: tv_rec = "Pembelian"
        else:               tv_rec = "Pembelian Kuat"

        # ---------- BULLISH PATTERNS (with tier) ----------
        BASE_PROB = {
            'bull_abandoned':0.65,'3ws':0.60,'morning_star':0.60,
            '3_outside_up':0.58,'bull_kicker':0.58,'bull_island':0.57,
            '3_inside_up':0.56,'rising_3':0.55,
            'bull_engulf':0.54,'piercing':0.53,'tweezer_bottom':0.52,
            'bull_harami':0.51,'hammer':0.52,'dragonfly_doji':0.50,'inv_hammer':0.50,
        }

        pola, tier, direction, pat_score = "-", 0, "neutral", 0.0

        # --- helper tier calc ---
        def calc_tier(factors):
            s = sum(factors)
            return 3 if s>=4 else 2 if s>=2 else 1

        # 3 White Soldiers
        vol_acc3ws = (day1['Volume']>day1['VOL_SMA_20']) and (day2['Volume']>=day1['Volume']) and (day3['Volume']>=day2['Volume'])
        if (is_down_basic and bull1 and bull2 and bull3 and
            body1>=0.5*range1 and body2>=0.5*range2 and body3>=0.5*range3 and
            day2['Close']>day1['Close'] and day3['Close']>day2['Close'] and
            day2['Open']>=day1['Open'] and day2['Open']<=day1['Close'] and
            day3['Open']>=day2['Open'] and day3['Open']<=day2['Close'] and
            close_near_high(day1) and close_near_high(day2) and close_near_high(day3) and candle_ok):
            t = calc_tier([vol_acc3ws, ext_os, near_sup, macd_b, is_volatile])
            if t>tier: pola,tier,direction = "Bullish: 3 White Soldiers",t,"bullish"

        # Abandoned Baby Bullish
        mid1_ab = (day1['Open']+day1['Close'])/2
        if (is_down_basic and bear1 and bull3 and
            body1>=0.5*range1 and body3>=0.5*range3 and
            (range2>0 and body2<=0.1*range2) and
            day2['High']<day1['Low'] and day2['High']<day3['Low'] and
            day3['Close']>mid1_ab):
            t = calc_tier([ext_os, near_sup, vol_thrust, macd_b, True])
            if t>tier: pola,tier,direction = "Bullish: Abandoned Baby",t,"bullish"

        # Morning Star
        sm2 = (range2>0) and (body2<=0.3*range2)
        if (is_proper_down and bear1 and range1>0 and body1>=0.6*range1 and sm2 and
            bull3 and range3>0 and body3>=0.6*range3 and
            day3['Close']>=mid1 and candle_ok):
            t = calc_tier([ext_os, near_sup, vol_thrust, macd_b, is_volatile])
            if t>tier: pola,tier,direction = "Bullish: Morning Star",t,"bullish"

        # 3 Inside Up
        bhi = (is_down_basic and bear1 and body1>=0.5*range1 and bull2 and
               body2<=0.6*body1 and bt2<=bt1 and bb2>=bb1)
        if bhi and bull3 and day3['Close']>day1['High']:
            t = calc_tier([os_ctx, near_sup, vol_above, macd_b])
            if t>tier: pola,tier,direction = "Bullish: 3 Inside Up",t,"bullish"

        # 3 Outside Up
        if (is_down_basic and bear1 and bull2 and
            day2['Close']>day1['Open'] and day2['Open']<day1['Close'] and body2>body1*1.2 and
            bull3 and body3>=0.5*range3 and day3['Close']>day2['High'] and candle_ok):
            t = calc_tier([os_ctx, near_sup, vol_thrust, macd_b])
            if t>tier: pola,tier,direction = "Bullish: 3 Outside Up",t,"bullish"

        # Bullish Engulfing
        if (bear2 and bull3 and day3['Close']>day2['Open'] and day3['Open']<day2['Close'] and
            body3>body2*1.08 and body2>=0.5*range2 and body3>=0.5*range3 and
            day3['Close']<day3['SMA_50'] and candle_ok):
            t = calc_tier([ext_os, near_sup, vol_thrust, ranging, macd_b])
            if t>tier: pola,tier,direction = "Bullish: Engulfing",t,"bullish"

        # Bullish Kicker
        if (bear2 and body2>=0.5*range2 and bull3 and body3>=0.5*range3 and
            day3['Open']>day2['High'] and day3['Low']>day2['High'] and candle_ok):
            t = calc_tier([os_ctx, near_sup, vol_thrust, macd_b])
            if t>tier: pola,tier,direction = "Bullish: Kicker",t,"bullish"

        # Island Reversal Bullish
        if (d1['Low']>max(d2['High'],d3['High'],d4['High']) and
            d5['Low']>max(d2['High'],d3['High'],d4['High']) and d5['Close']>d5['Open']):
            t = calc_tier([os_ctx, vol_thrust, macd_b])
            if t>tier: pola,tier,direction = "Bullish: Island Reversal",t,"bullish"

        # Piercing Line
        bb_pl = day2['Open']-day2['Close']
        pr_pl = (day3['Close']-day2['Close'])/bb_pl if bb_pl>0 else 0
        if (is_proper_down and bear2 and bb_pl>=0.6*range2 and bull3 and
            day3['Open']<day2['Close']*0.999 and pr_pl>=0.5 and day3['Close']<day2['Open'] and candle_ok):
            t = calc_tier([ext_os, near_sup, vol_thrust, macd_b])
            if t>tier: pola,tier,direction = "Bullish: Piercing Line",t,"bullish"

        # Tweezer Bottom
        if (is_down_basic and bear2 and body2>=0.5*range2 and bull3 and body3>=0.5*range3 and
            is_near(day2['Low'],day3['Low']) and day3['Close']>day2['Close'] and
            (range3>=atr3*0.7 if valid_atr else True)):
            t = calc_tier([os_ctx, near_sup, vol_above, macd_b])
            if t>tier: pola,tier,direction = "Bullish: Tweezer Bottom",t,"bullish"

        # Bullish Harami
        if (is_down_basic and bear2 and body2>=0.5*range2 and bull3 and
            body3>=0.2*range3 and body3<=0.6*body2 and bt3<=bt2 and bb3>=bb2):
            t = calc_tier([os_ctx, near_sup, vol_above, macd_b])
            if t>tier: pola,tier,direction = "Bullish: Harami",t,"bullish"

        # Hammer
        if (range3>0 and body3>0 and body3<=0.3*range3 and ls3>=2*body3 and
            us3<=0.15*range3 and (day3['High']-day3['Close'])<=0.25*range3 and
            day3['Close']<day3['SMA_20'] and candle_ok):
            t = calc_tier([is_proper_down, ext_os, near_sup, vol_above])
            if t>tier: pola,tier,direction = "Bullish: Hammer",t,"bullish"

        # Inverted Hammer
        if (range3>0 and body3>0 and body3<=0.3*range3 and us3>=2*body3 and
            ls3<=0.15*range3 and (day3['Close']-day3['Low'])<=0.25*range3 and
            day3['SMA_20']<day3['SMA_50'] and candle_ok):
            t = calc_tier([is_proper_down, os_ctx, near_sup])
            if t>tier: pola,tier,direction = "Bullish: Inverted Hammer",t,"bullish"

        # Dragonfly Doji
        is_doji = range3>0 and body3<=0.08*range3
        if is_doji and ls3>=0.7*range3 and us3<=0.1*range3:
            t = calc_tier([os_ctx, near_sup])
            if t>tier: pola,tier,direction = "Bullish: Dragonfly Doji",t,"bullish"

        # ---------- Hitung skor pola ----------
        if direction == "bullish" and tier > 0:
            key = {
                "Bullish: 3 White Soldiers":"3ws","Bullish: Abandoned Baby":"bull_abandoned",
                "Bullish: Morning Star":"morning_star","Bullish: 3 Inside Up":"3_inside_up",
                "Bullish: 3 Outside Up":"3_outside_up","Bullish: Engulfing":"bull_engulf",
                "Bullish: Kicker":"bull_kicker","Bullish: Island Reversal":"bull_island",
                "Bullish: Piercing Line":"piercing","Bullish: Tweezer Bottom":"tweezer_bottom",
                "Bullish: Harami":"bull_harami","Bullish: Hammer":"hammer",
                "Bullish: Inverted Hammer":"inv_hammer","Bullish: Dragonfly Doji":"dragonfly_doji",
            }.get(pola, "bull_engulf")
            cf = sum([near_sup, ext_os, vol_thrust, macd_b, os_ctx])
            pat_score = get_pattern_score(BASE_PROB.get(key,0.51), tier, cf)

        # S/R label
        sr_label = "Support" if near_sup else "Resistance" if pp>0.75 else "-"

        return {
            "pola"      : pola + (" " + get_tier_stars(tier) if tier>0 else ""),
            "tier"      : tier,
            "direction" : direction,
            "pat_score" : pat_score,
            "tv_rec"    : tv_rec,
            "sr_zone"   : sr_label,
            "vol_thrust": vol_thrust,
            "sl_price"  : round(day3['Low'], 2),   # SL = Low candle sinyal
            "signal_date": df.index[idx],
        }
    except Exception as e:
        return None

# ============================================================
# BACKTESTING ENGINE
# ============================================================
def run_backtest(ticker, df_full, start_date, end_date):
    """
    Jalankan backtesting untuk satu ticker.
    Return list of trade dicts.
    """
    trades = []

    # Filter rentang backtest
    df = df_full[(df_full.index >= start_date) & (df_full.index <= end_date)].copy()
    if len(df) < 60:
        return trades

    # Re-index df_full untuk akses by iloc
    df_all = df_full.copy()

    # Iterasi setiap baris sebagai "hari sinyal"
    for i in range(5, len(df)-1):
        # Posisi absolut di df_full
        full_idx = df_all.index.get_loc(df.index[i])
        if full_idx < 5:
            continue

        sig = generate_signal(df_all, full_idx)
        if sig is None:
            continue

        # ---- FILTER ENTRY CONDITIONS ----
        if sig['direction'] != "bullish":           continue
        if sig['tier'] = 3:                         continue
        if sig['pat_score'] <= 65:                  continue
        if not sig['vol_thrust']:                   continue

        # Entry di Open hari berikutnya
        next_idx_in_df = i + 1
        if next_idx_in_df >= len(df):
            continue

        entry_date  = df.index[next_idx_in_df]
        entry_price = df['Open'].iloc[next_idx_in_df]

        if entry_price <= 0 or pd.isna(entry_price):
            continue

        sl_price = sig['sl_price']
        risk_per_share = entry_price - sl_price

        if risk_per_share <= 0:
            continue

        tp_price = entry_price + (risk_per_share * RR_TARGET)

        trades.append({
            "ticker"      : ticker,
            "signal_date" : sig['signal_date'],
            "entry_date"  : entry_date,
            "entry_price" : entry_price,
            "sl_price"    : sl_price,
            "tp_price"    : tp_price,
            "risk_per_share": risk_per_share,
            "pola"        : sig['pola'],
            "tier"        : sig['tier'],
            "pat_score"   : sig['pat_score'],
            "tv_rec"      : sig['tv_rec'],
            # exit fields (diisi kemudian)
            "exit_date"   : None,
            "exit_price"  : None,
            "exit_reason" : None,
            # simulation entry index in df
            "_entry_idx"  : next_idx_in_df,
            "_df_ref"     : df,
        })

    return trades

def simulate_exit(trade):
    """
    Simulasikan exit untuk satu trade.
    Cek setiap hari setelah entry apakah TP/SL tercapai.
    """
    df          = trade['_df_ref']
    entry_idx   = trade['_entry_idx']
    entry_price = trade['entry_price']
    sl_price    = trade['sl_price']
    tp_price    = trade['tp_price']

    exit_date   = None
    exit_price  = None
    exit_reason = None

    for j in range(1, MAX_HOLD_DAYS + 1):
        hold_idx = entry_idx + j
        if hold_idx >= len(df):
            # Habis data — exit di close terakhir yang ada
            last_idx = len(df) - 1
            exit_date   = df.index[last_idx]
            exit_price  = df['Close'].iloc[last_idx]
            exit_reason = "Time Exit"
            break

        day_high  = df['High'].iloc[hold_idx]
        day_low   = df['Low'].iloc[hold_idx]
        day_close = df['Close'].iloc[hold_idx]
        day_open  = df['Open'].iloc[hold_idx]

        # SL check (intraday — worst case low menyentuh SL)
        hit_sl = day_low <= sl_price
        # TP check
        hit_tp = day_high >= tp_price

        if hit_sl and hit_tp:
            # Kedua kena hari yang sama — lihat mana yang lebih mungkin duluan
            # Jika open sudah di bawah SL → SL hit pertama
            if day_open <= sl_price:
                exit_date, exit_price, exit_reason = df.index[hold_idx], min(day_open, sl_price), "SL"
            else:
                # Asumsikan TP hit dulu (konservatif untuk overshooting)
                exit_date, exit_price, exit_reason = df.index[hold_idx], tp_price, "TP"
            break
        elif hit_sl:
            exit_date   = df.index[hold_idx]
            exit_price  = sl_price
            exit_reason = "SL"
            break
        elif hit_tp:
            exit_date   = df.index[hold_idx]
            exit_price  = tp_price
            exit_reason = "TP"
            break
        elif j == MAX_HOLD_DAYS:
            exit_date   = df.index[hold_idx]
            exit_price  = day_close
            exit_reason = "Time Exit"
            break

    if exit_date is None:
        exit_date   = df.index[min(entry_idx + MAX_HOLD_DAYS, len(df)-1)]
        exit_price  = df['Close'].iloc[min(entry_idx + MAX_HOLD_DAYS, len(df)-1)]
        exit_reason = "Time Exit"

    trade['exit_date']   = exit_date
    trade['exit_price']  = exit_price
    trade['exit_reason'] = exit_reason
    return trade

# ============================================================
# PORTFOLIO SIMULATION (1 posisi aktif, compounding)
# ============================================================
def simulate_portfolio(all_signals):
    """
    Dari semua sinyal semua ticker, simulasikan portofolio:
    - 1 posisi aktif sekaligus
    - Full compounding
    - Urut berdasarkan entry_date, prioritas tier+skor tertinggi
    """
    # Urutkan sinyal: entry_date ↑, tier ↓, pat_score ↓
    all_signals.sort(key=lambda x: (x['entry_date'], -x['tier'], -x['pat_score']))

    equity    = float(MODAL_AWAL)
    portfolio = []
    active_end = None   # tanggal exit posisi aktif

    for trade in all_signals:
        ed = trade['entry_date']

        # Skip jika masih ada posisi aktif
        if active_end is not None and ed <= active_end:
            continue

        # Hitung lot dengan modal saat ini
        entry_price = trade['entry_price']
        sl_price    = trade['sl_price']
        risk_ps     = trade['risk_per_share']

        # Jumlah lot maksimal yang bisa dibeli
        max_shares    = int((equity / entry_price) // LOT_SIZE) * LOT_SIZE
        if max_shares <= 0:
            continue

        lot_count     = max_shares // LOT_SIZE
        total_shares  = lot_count * LOT_SIZE
        total_invest  = total_shares * entry_price

        # Simulasikan exit
        trade = simulate_exit(trade)

        exit_price  = trade['exit_price']
        exit_reason = trade['exit_reason']
        exit_date   = trade['exit_date']

        # Hitung P&L
        pnl_rp      = (exit_price - entry_price) * total_shares
        return_pct  = ((exit_price - entry_price) / entry_price) * 100
        r_multiple  = (exit_price - entry_price) / risk_ps if risk_ps > 0 else 0

        equity_after = equity + pnl_rp
        active_end   = exit_date

        portfolio.append({
            "Ticker"           : trade['ticker'],
            "TV Rec"           : trade['tv_rec'],
            "Pola"             : trade['pola'],
            "Tier"             : trade['tier'],
            "Skor Pola"        : trade['pat_score'],
            "Entry Date"       : ed.strftime("%Y-%m-%d") if hasattr(ed,'strftime') else str(ed),
            "Entry Price"      : round(entry_price, 0),
            "Lot Size"         : lot_count,
            "Total Investasi"  : round(total_invest, 0),
            "Stop Loss"        : round(sl_price, 0),
            "Target (TP)"      : round(trade['tp_price'], 0),
            "Exit Date"        : exit_date.strftime("%Y-%m-%d") if hasattr(exit_date,'strftime') else str(exit_date),
            "Exit Price"       : round(exit_price, 0),
            "Exit Reason"      : exit_reason,
            "Profit/Loss (Rp)" : round(pnl_rp, 0),
            "Return %"         : round(return_pct, 2),
            "Equity Setelah"   : round(equity_after, 0),
            "R Multiple"       : round(r_multiple, 2),
        })

        equity = equity_after

    return portfolio, equity

# ============================================================
# SUMMARY STATISTICS
# ============================================================
def calc_summary(portfolio, final_equity):
    if not portfolio:
        return {}

    df = pd.DataFrame(portfolio)
    total_trades = len(df)
    wins  = df[df['Profit/Loss (Rp)'] > 0]
    loses = df[df['Profit/Loss (Rp)'] <= 0]
    win_rate   = len(wins) / total_trades * 100 if total_trades > 0 else 0

    avg_win_pct  = wins['Return %'].mean()  if len(wins)  > 0 else 0
    avg_loss_pct = loses['Return %'].mean() if len(loses) > 0 else 0

    gross_profit = wins['Profit/Loss (Rp)'].sum()  if len(wins)  > 0 else 0
    gross_loss   = abs(loses['Profit/Loss (Rp)'].sum()) if len(loses) > 0 else 0
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else float('inf')

    avg_r_win  = wins['R Multiple'].mean()  if len(wins)  > 0 else 0
    avg_r_loss = loses['R Multiple'].mean() if len(loses) > 0 else 0
    win_rate_dec = win_rate / 100
    expectancy   = (win_rate_dec * avg_r_win) + ((1-win_rate_dec) * avg_r_loss)

    # Max Drawdown
    equity_curve = [MODAL_AWAL] + df['Equity Setelah'].tolist()
    equity_series = pd.Series(equity_curve, dtype=float)
    rolling_max   = equity_series.cummax()
    drawdown      = (equity_series - rolling_max) / rolling_max * 100
    max_dd        = drawdown.min()

    # CAGR
    total_return = (final_equity - MODAL_AWAL) / MODAL_AWAL
    cagr = ((1 + total_return) ** (1/BACKTEST_YEARS) - 1) * 100

    return {
        "Modal Awal"       : f"Rp {MODAL_AWAL:,.0f}",
        "Modal Akhir"      : f"Rp {final_equity:,.0f}",
        "Total Return"     : f"{total_return*100:.2f}%",
        "Total Trade"      : total_trades,
        "Win Rate"         : f"{win_rate:.1f}%",
        "Total Win"        : len(wins),
        "Total Loss"       : len(loses),
        "Avg Win %"        : f"{avg_win_pct:.2f}%",
        "Avg Loss %"       : f"{avg_loss_pct:.2f}%",
        "Profit Factor"    : f"{profit_factor:.2f}",
        "Expectancy (R)"   : f"{expectancy:.3f}R",
        "Max Drawdown %"   : f"{max_dd:.2f}%",
        "CAGR 2 Tahun"     : f"{cagr:.2f}%",
        "Gross Profit"     : f"Rp {gross_profit:,.0f}",
        "Gross Loss"       : f"Rp {gross_loss:,.0f}",
    }

# ============================================================
# UPLOAD TO GOOGLE SHEETS
# ============================================================
def upload_results(portfolio, summary):
    print("\n📤 Upload ke Google Sheets...")

    # --- Sheet: Transaksi ---
    ws_tx = connect_gsheet("Transaksi")
    if ws_tx and portfolio:
        df_tx = pd.DataFrame(portfolio)
        ws_tx.clear()
        set_with_dataframe(ws_tx, df_tx)
        print(f"✅ Sheet 'Transaksi' — {len(df_tx)} baris diupload")

    time.sleep(2)

    # --- Sheet: Summary ---
    ws_sm = connect_gsheet("Summary")
    if ws_sm and summary:
        rows = [["Metrik", "Nilai"]] + [[k, v] for k, v in summary.items()]
        ws_sm.clear()
        ws_sm.update(rows)
        print("✅ Sheet 'Summary' diupload")

# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    print("=" * 60)
    print("🤖 BACKTESTING ENGINE — CANDLESTICK STRATEGY")
    print(f"   Modal Awal  : Rp {MODAL_AWAL:,.0f}")
    print(f"   Periode     : {BACKTEST_YEARS} Tahun")
    print(f"   Universe    : {len(ALL_TICKERS)} saham")
    print(f"   Max Hold    : {MAX_HOLD_DAYS} hari trading")
    print(f"   RR Target   : 1:{RR_TARGET}")
    print("=" * 60)

    end_date   = datetime.now()
    start_date = end_date - timedelta(days=BACKTEST_YEARS * 365 + 60)  # +60 hari buffer indikator

    # Tanggal efektif backtest (setelah buffer indikator)
    bt_start = end_date - timedelta(days=BACKTEST_YEARS * 365)

    all_signals = []
    failed      = []

    print(f"\n📊 Download & scan {len(ALL_TICKERS)} saham...")
    for i, ticker in enumerate(ALL_TICKERS):
        try:
            df_raw = yf.download(
                ticker,
                start=start_date.strftime("%Y-%m-%d"),
                end=end_date.strftime("%Y-%m-%d"),
                interval="1d",
                progress=False,
                auto_adjust=True
            )
            if df_raw.empty or len(df_raw) < 60:
                continue
            if isinstance(df_raw.columns, pd.MultiIndex):
                df_raw.columns = df_raw.columns.droplevel(1)
            df_raw.dropna(inplace=True)

            # Hitung indikator
            df_ind = calc_indicators(df_raw.copy())
            df_ind.dropna(inplace=True)

            if len(df_ind) < 60:
                continue

            # Generate sinyal
            signals = run_backtest(ticker, df_ind, bt_start, end_date)
            all_signals.extend(signals)

            if (i+1) % 20 == 0:
                print(f"   [{i+1}/{len(ALL_TICKERS)}] Sinyal terkumpul: {len(all_signals)}")

        except Exception as e:
            failed.append(ticker)
            continue

    print(f"\n✅ Total sinyal kandidat  : {len(all_signals)}")
    print(f"⚠️  Ticker gagal download  : {len(failed)}")

    if not all_signals:
        print("❌ Tidak ada sinyal yang memenuhi syarat. Backtest dihentikan.")
        exit()

    # Simulasi portofolio
    print("\n🔄 Simulasi portofolio (1 posisi aktif, full compounding)...")
    portfolio, final_equity = simulate_portfolio(all_signals)

    print(f"✅ Total trade dieksekusi : {len(portfolio)}")
    print(f"   Modal Awal             : Rp {MODAL_AWAL:,.0f}")
    print(f"   Modal Akhir            : Rp {final_equity:,.0f}")

    # Hitung summary
    summary = calc_summary(portfolio, final_equity)

    # Print summary ke console
    print("\n" + "=" * 60)
    print("📈 RINGKASAN HASIL BACKTEST")
    print("=" * 60)
    for k, v in summary.items():
        print(f"   {k:<25} : {v}")
    print("=" * 60)

    # Upload ke Google Sheets
    upload_results(portfolio, summary)

    print("\n🏁 BACKTEST SELESAI")
