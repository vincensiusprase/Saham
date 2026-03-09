"""
IHSG Channel Breakout Screener v5.0 (Complete Trading System - Trial 10 Tickers)
=============================================================
Strategy: Channel Breakout (Donchian Channel style)
"""

import os, sys, json, time, warnings
from datetime import datetime, timezone, timedelta

import numpy as np
import pandas as pd
import yfinance as yf
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials

warnings.filterwarnings("ignore")

# ── CONFIG ─────────────────────────────────────────────────────────────────
SPREADSHEET_ID  = "1qhEZkfdtEGDEA5tWTVCeOahOXDso3H4O7B69xkOREDA"
SUMMARY_SHEET   = "SCREENER"
SERVICE_ACCOUNT = "service_account.json"
LENGTH          = 20  # Menggantikan ATR_LENGTH & FACTOR
DOWNLOAD_DAYS   = 365
WIB             = timezone(timedelta(hours=7))

# ── SECTORS (TRIAL: Max 10 Tickers / Sector) ───────────────────────────────
SECTORS = {
        "IDXINDUST": [
        "ASII.JK", "UNTR.JK", "PIPA.JK", "BNBR.JK", "HEXA.JK", "IMPC.JK", "MHKI.JK", "LABA.JK", "SMIL.JK", "NTBK.JK", 
        "PADA.JK", "NAIK.JK", "TOTO.JK", "BHIT.JK", "MARK.JK", "INDX.JK", "PTMP.JK", "ARNA.JK", "KOBX.JK", "ASGR.JK", 
        "GPSO.JK", "SINI.JK", "HOPE.JK", "FOLK.JK", "KUAS.JK", "KBLI.JK", "SPTO.JK", "IKAI.JK", "SKRN.JK", "MUTU.JK", 
        "CTTH.JK", "CAKK.JK", "CCSI.JK", "MLIA.JK", "JTPE.JK", "BLUE.JK", "CRSN.JK", "DYAN.JK", "LION.JK", "MDRN.JK", 
        "VOKS.JK", "SCCO.JK", "KBLM.JK", "JECC.JK", "INTA.JK", "BINO.JK", "VISI.JK", "ARKA.JK", "SOSS.JK", "IKBI.JK", 
        "AMIN.JK", "ICON.JK", "KONI.JK", "AMFG.JK", "IBFN.JK", "ZBRA.JK", "TIRA.JK", "KIAS.JK", "APII.JK", "HYGN.JK", 
        "KOIN.JK", "MFMI.JK", "TRIL.JK"
    ],
    "IDXNONCYC": [
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
    ],
   "IDXFINANCE": [
    "BBCA.JK", "BBRI.JK", "BMRI.JK", "BBNI.JK", "BRIS.JK", "SUPA.JK", "COIN.JK", "BBTN.JK", "ARTO.JK", "BBYB.JK", "BNGA.JK", "BBKP.JK",
    "BTPS.JK", "BJTM.JK", "SRTG.JK", "PNLF.JK", "PADI.JK", "AGRO.JK", "NISP.JK", "INPC.JK", "BJBR.JK", "BBHI.JK", "BFIN.JK", "BDMN.JK",
    "BABP.JK", "PNBS.JK", "BGTG.JK", "AHAP.JK", "BANK.JK", "BACA.JK", "BNLI.JK", "BNII.JK", "BCAP.JK", "PNBN.JK", "MEGA.JK", "BVIC.JK",
    "ADMF.JK", "DNAR.JK", "MAYA.JK", "CFIN.JK", "BTPN.JK", "BSIM.JK", "BEKS.JK", "TUGU.JK", "PEGE.JK", "NOBU.JK", "PALM.JK", "BNBA.JK",
    "LPPS.JK", "AGRS.JK", "DNET.JK", "AMAR.JK", "GSMF.JK", "JMAS.JK", "TRIM.JK", "MCOR.JK", "PNIN.JK", "SMMA.JK", "PANS.JK", "BKSW.JK",
    "VINS.JK", "BCIC.JK", "BINA.JK", "WOMF.JK", "LPGI.JK", "LIFE.JK", "VTNY.JK", "VICO.JK", "STAR.JK", "YOII.JK", "FUJI.JK", "MTWI.JK",
    "POLA.JK", "BBSI.JK", "ASJT.JK", "SDRA.JK", "BMAS.JK", "AMAG.JK", "ASMI.JK", "HDFA.JK", "VRNA.JK", "AMOR.JK", "APIC.JK", "MREI.JK",
    "ASDM.JK", "TIFA.JK", "BHIT.JK", "ASRM.JK", "RELI.JK", "NICK.JK", "TRUS.JK", "ASBI.JK", "DEFI.JK", "BBLD.JK", "BBMD.JK", "MASB.JK",
    "BPFI.JK", "YULE.JK", "BPII.JK", "POOL.JK", "BSWD.JK", "SFAN.JK", "ABDA.JK", "OCAP.JK", "PLAS.JK"
    ],
    "IDXCYCLIC": [
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
    ],
    "IDXTECHNO": [
    "GOTO.JK", "WIFI.JK", "EMTK.JK", "BUKA.JK", "WIRG.JK", "DCII.JK", "IOTF.JK", "MTDL.JK", "ELIT.JK", "MLPT.JK", "DMMX.JK",
    "TOSK.JK", "JATI.JK", "KIOS.JK", "IRSX.JK", "UVCR.JK", "TRON.JK", "KREN.JK", "CYBR.JK", "LUCK.JK", "PTSN.JK", "HDIT.JK",
    "EDGE.JK", "DIVA.JK", "TFAS.JK", "ZYRX.JK", "MSTI.JK", "MCAS.JK", "MPIX.JK", "BELI.JK", "AXIO.JK", "AWAN.JK", "AREA.JK",
    "NFCX.JK", "ATIC.JK", "TECH.JK", "GLVA.JK", "ENVY.JK", "LMAS.JK", "SKYB.JK"
    ],
    "IDXBASIC": [
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
    ],
    "IDXENERGY": [
    "ADRO.JK", "BUMI.JK", "PGAS.JK", "PTBA.JK", "ITMG.JK", "DEWA.JK", "CUAN.JK", "HRUM.JK", "PTRO.JK", "RAJA.JK", "MEDC.JK", "ADMR.JK",
    "HUMI.JK", "ENRG.JK", "BULL.JK", "TOBA.JK", "AADI.JK", "RATU.JK", "CBRE.JK", "INDY.JK", "AKRA.JK", "ELSA.JK", "GTSI.JK", "BIPI.JK",
    "COAL.JK", "BSSR.JK", "LEAD.JK", "APEX.JK", "TEBE.JK", "ATLA.JK", "SOCI.JK", "FIRE.JK", "PSAT.JK", "GEMS.JK", "DOID.JK", "DSSA.JK",
    "SGER.JK", "IATA.JK", "BBRM.JK", "BYAN.JK", "ABMM.JK", "TPMA.JK", "MAHA.JK", "BOAT.JK", "KKGI.JK", "MBSS.JK", "WOWS.JK", "CGAS.JK",
    "RMKE.JK", "WINS.JK", "MTFN.JK", "MBAP.JK", "UNIQ.JK", "RMKO.JK", "SMMT.JK", "SICO.JK", "BSML.JK", "PSSI.JK", "DWGL.JK", "TAMU.JK",
    "ALII.JK", "ITMA.JK", "RUIS.JK", "CNKO.JK", "TCPI.JK", "HILL.JK", "BOSS.JK", "PKPK.JK", "MYOH.JK", "SEMA.JK", "ARII.JK", "GTBO.JK",
    "MCOL.JK", "RGAS.JK", "SHIP.JK", "BESS.JK", "RIGS.JK", "JSKY.JK", "KOPI.JK", "PTIS.JK", "CANI.JK", "ARTI.JK", "INPS.JK", "MKAP.JK",
    "AIMS.JK", "HITS.JK", "SUNI.JK", "TRAM.JK", "SURE.JK", "SMRU.JK", "SUGI.JK"
    ],
    "IDXHEALTH": [
    "KLBF.JK", "SIDO.JK", "KAEF.JK", "PYFA.JK", "MIKA.JK", "DKHH.JK", "SILO.JK", "HEAL.JK", "TSPC.JK", "INAF.JK", "CHEK.JK", "IRRA.JK", "SAME.JK", "MEDS.JK", "PRDA.JK", "MDLA.JK", "SURI.JK", "PRIM.JK", "HALO.JK", "OBAT.JK", "CARE.JK",
    "MERK.JK", "DGNS.JK", "SOHO.JK", "BMHS.JK", "PEHA.JK", "SRAJ.JK", "MMIX.JK", "DVLA.JK", "OMED.JK", "PEVE.JK", "LABS.JK", "RSCH.JK", "MTMH.JK", "IKPM.JK", "PRAY.JK", "SCPI.JK", "RSGK.JK"
    ],
    "IDXINFRA": [
    "TLKM.JK", "CDIA.JK", "ADHI.JK", "JSMR.JK", "WIKA.JK", "PTPP.JK", "INET.JK", "WSKT.JK", "BREN.JK", "PGEO.JK", "EXCL.JK", "ISAT.JK", "TOWR.JK", "SSIA.JK", "DATA.JK", "OASA.JK", "PPRE.JK", "TBIG.JK", "POWR.JK", "NRCA.JK", "WEGE.JK", "TOTL.JK",
    "KETR.JK", "IPCC.JK", "KOKA.JK", "KBLV.JK", "MTEL.JK", "CENT.JK", "KRYA.JK", "GMFI.JK", "JAST.JK", "KEEN.JK", "JKON.JK", "ACST.JK", "ASLI.JK", "PBSA.JK", "IPCM.JK", "MORA.JK", "ARKO.JK", "MPOW.JK", "CMNP.JK", "LINK.JK", "HGII.JK", "DGIK.JK", "BDKR.JK",
    "META.JK", "KARW.JK", "CASS.JK", "BUKK.JK", "TGRA.JK", "GOLD.JK", "BALI.JK", "PTDU.JK", "IDPR.JK", "PORT.JK", "TOPS.JK", "HADE.JK", "TAMA.JK", "BTEL.JK", "GHON.JK", "SUPR.JK", "MTPS.JK", "RONY.JK", "IBST.JK", "LCKM.JK", "PTPW.JK", "MTRA.JK"
    ],
    "IDXPROPERT": [
    "CTRA.JK", "BSDE.JK", "PWON.JK", "SMRA.JK", "KLJA.JK", "PANI.JK", "BKSL.JK", "DADA.JK", "CBDK.JK", "DMAS.JK", "ASRI.JK", "LPKR.JK", "BSBK.JK", "REAL.JK", "ELTY.JK", "APLN.JK", "TRUE.JK", "TRIN.JK", "UANG.JK", "CSIS.JK", "DILD.JK", "KOCI.JK", "BEST.JK",
    "LAND.JK", "DUTI.JK", "EMDE.JK", "LPLI.JK", "GRIA.JK", "VAST.JK", "BAPI.JK", "MTLA.JK", "SAGE.JK", "BBSS.JK", "HOMI.JK", "PUDP.JK", "RBMS.JK", "URBN.JK", "TARA.JK", "CBPE.JK", "MPRO.JK", "RODA.JK", "SATU.JK", "NASA.JK", "FMII.JK", "BKDP.JK", "GMTD.JK",
    "PPRO.JK", "BAPA.JK", "PAMG.JK", "MMLP.JK", "PURI.JK", "GPRA.JK", "LPCK.JK", "MDLN.JK", "BCIP.JK", "ADCP.JK", "CITY.JK", "RISE.JK", "WINR.JK", "JRPT.JK", "AMAN.JK", "SMDM.JK", "INDO.JK", "ATAP.JK", "ASPI.JK", "KSIX.JK", "KBAG.JK", "NZIA.JK",
    "NIRO.JK", "DART.JK", "BIPP.JK", "PLIN.JK", "RDTX.JK", "ROCK.JK", "MKPI.JK", "INPP.JK", "MTSM.JK", "POLL.JK", "POLI.JK", "OMRE.JK", "GAMA.JK", "POSA.JK", "BIKA.JK", "CPRI.JK", "ARMY.JK", "COWL.JK", "RIMO.JK", "LCGP.JK"
    ],
    "IDXTRANS": [
    "PJHB.JK", "GIAA.JK", "SMDR.JK", "BIRD.JK", "BLOG.JK", "IMJS.JK", "ASSA.JK", "TMAS.JK", "LAJU.JK", "HAIS.JK", "KLAS.JK", "MITI.JK", "JAYA.JK", "NELY.JK", "WEHA.JK", "TNCA.JK", "CMPP.JK", "MPXL.JK", "KJEN.JK", "SDMU.JK", "TRUK.JK", "PURA.JK", "HATM.JK",
    "TAXI.JK", "ELPI.JK", "AKSI.JK", "GTRA.JK", "TRJA.JK", "MIRA.JK", "BLTA.JK", "SAPX.JK", "SAFE.JK", "LRNA.JK", "DEAL.JK", "BPTR.JK", "HELI.JK"
    ]
}

# ── COMMODITIES ────────────────────────────────────────────────────────────
COMMODITIES = {
    "Gold"      : {"t":"GC=F",     "ma":20, "s":["IDXBASIC","IDXFINANCE"]},
    "Silver"    : {"t":"SI=F",     "ma":20, "s":["IDXBASIC","IDXINDUST"]},
    "Copper"    : {"t":"HG=F",     "ma":20, "s":["IDXBASIC","IDXINDUST","IDXINFRA"]},
    "Nickel"    : {"t":"NI=F",     "ma":20, "s":["IDXBASIC","IDXINDUST"]},
    "Aluminium" : {"t":"ALI=F",    "ma":20, "s":["IDXBASIC","IDXINDUST"]},
    "Zinc"      : {"t":"ZNC=F",    "ma":20, "s":["IDXBASIC","IDXINDUST"]},
    "Tin"       : {"t":"JJT",      "ma":20, "s":["IDXBASIC"]},
    "Brent Oil" : {"t":"BZ=F",     "ma":20, "s":["IDXENERGY","IDXNONCYC","IDXTRANS"]},
    "Crude Oil" : {"t":"CL=F",     "ma":20, "s":["IDXENERGY","IDXNONCYC","IDXTRANS"]},
    "Nat Gas"   : {"t":"NG=F",     "ma":20, "s":["IDXENERGY","IDXINFRA"]},
    "Coal"      : {"t":"KOL",      "ma":20, "s":["IDXENERGY","IDXBASIC"]},
    "CPO"       : {"t":"FCPO.KL",  "ma":20, "s":["IDXNONCYC","IDXBASIC"]},
    "Corn"      : {"t":"ZC=F",     "ma":20, "s":["IDXNONCYC"]},
    "Wheat"     : {"t":"ZW=F",     "ma":20, "s":["IDXNONCYC"]},
    "DXY"       : {"t":"DX-Y.NYB", "ma":20, "s":["ALL"]},
    "IHSG"      : {"t":"^JKSE",    "ma":50, "s":["ALL"]},
}
FALLBACK = {"Nickel":"DBB","Aluminium":"DBB","Zinc":"DBB",
            "Tin":"JJT","Coal":"ARCH","CPO":"POW.L"}

DISPLAY_COLS = [
    "Ticker", "Kategori Strategi", "Sektor", "Action", "Harga", "Batas Jual (SL)", 
    "Channel Breakout Signal", "Tgl Channel Breakout", "Skor Tambahan", "ADTV (M)",
    "Skor TV", "Rek TV", "Alasan Rek TV",
    "Commodity Bullish %", "Commodity Context",
]

# ── GOOGLE SHEETS ──────────────────────────────────────────────────────────
def gsheet(name):
    try:
        raw = os.environ.get("GCP_SA_KEY") or os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
        if raw:
            creds = Credentials.from_service_account_info(
                json.loads(raw),
                scopes=["https://www.googleapis.com/auth/spreadsheets",
                        "https://www.googleapis.com/auth/drive"])
        elif os.path.exists(SERVICE_ACCOUNT):
            creds = Credentials.from_service_account_file(
                SERVICE_ACCOUNT,
                scopes=["https://spreadsheets.google.com/feeds",
                        "https://www.googleapis.com/auth/drive"])
        else:
            print("❌ No credentials"); return None
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(SPREADSHEET_ID)
        try:    return sh.worksheet(name)
        except: return sh.add_worksheet(title=name, rows="500", cols="15")
    except Exception as e:
        print(f"❌ GSheet: {e}"); return None

# ── DATA DOWNLOAD ──────────────────────────────────────────────────────────
def get_ohlcv(ticker, days=DOWNLOAD_DAYS):
    end   = datetime.today()
    start = end - timedelta(days=days)

    try:
        df = yf.download(
            ticker,
            start=start.strftime("%Y-%m-%d"),
            end=end.strftime("%Y-%m-%d"),
            interval="1d",
            progress=False,
            auto_adjust=False,
        )
    except Exception as e:
        return None

    if df is None or df.empty:
        return None

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] for c in df.columns]

    if "Close" not in df.columns and "Adj Close" in df.columns:
        df["Close"] = df["Adj Close"]

    needed = ["High", "Low", "Close", "Volume"]
    if not all(c in df.columns for c in needed):
        return None

    df = df[["Open","High","Low","Close","Volume"]].dropna(
        subset=["High","Low","Close"]).copy()
    df.index = pd.to_datetime(df.index)
    df.sort_index(inplace=True)
    return df

# ── STRATEGY CLASSIFICATION ────────────────────────────────────────────────
def get_strategy_category(sector, current_price, adtv_miliar):
    if adtv_miliar >= 50 and sector in ["IDXFINANCE", "IDXENERGY"]:
        if sector == "IDXFINANCE" and current_price >= 2000:
            return "Trend-Following"
        return "Price Action / Reversal"
        
    if current_price >= 2000 and sector in ["IDXFINANCE", "IDXINFRA"]:
        return "Trend-Following"
        
    if 100 <= current_price <= 1500 and sector in ["IDXENERGY", "IDXBASIC", "IDXTECHNO"]:
        return "Breakout & Volatility"
        
    if 500 <= current_price <= 3000 and sector in ["IDXNONCYC", "IDXHEALTH"]:
        return "Mean Reversion"
        
    if sector in ["IDXNONCYC", "IDXHEALTH", "IDXPROPERT"]:
        return "Mean Reversion"
    elif sector in ["IDXENERGY", "IDXBASIC", "IDXTECHNO", "IDXCYCLIC", "IDXINDUST", "IDXTRANS"]:
        return "Breakout & Volatility"
    else:
        return "Trend-Following"

# ── CHANNEL BREAKOUT (PERBAIKAN AKURASI) ───────────────────────────────────
def calc_cb(df):
    up   = df["High"].rolling(LENGTH).max().shift(1)
    down = df["Low"].rolling(LENGTH).min().shift(1)

    le_condition = df["High"] > up
    se_condition = df["Low"]  < down

    df_sig = pd.DataFrame(index=df.index)
    conditions = [le_condition, se_condition]
    choices = [1, -1] 
    
    df_sig['state'] = np.select(conditions, choices, default=np.nan)
    df_sig['state'] = df_sig['state'].ffill()

    df_sig['entry_long']  = (df_sig['state'] == 1)  & (df_sig['state'].shift(1) != 1)
    df_sig['entry_short'] = (df_sig['state'] == -1) & (df_sig['state'].shift(1) != -1)
    
    # SL for Channel Breakout is the opposite band
    df_sig['sl_price'] = np.where(df_sig['state'] == 1, down, up)

    def get_last_signal(series):
        arr = series.values
        idx = df.index
        for i in range(len(arr)-1, -1, -1):
            if arr[i]:
                bars = len(arr) - 1 - i
                return idx[i], bars
        return None, None

    le_dt, le_b = get_last_signal(df_sig['entry_long'])
    se_dt, se_b = get_last_signal(df_sig['entry_short'])

    def fmt_label(bars, lbl):
        ago = "hari ini" if bars == 0 else f"{bars} bar lalu"
        return f"{lbl} ({ago})"

    def fmt_date(dt):
        if dt is None: return "-"
        return (pd.Timestamp(dt) + pd.Timedelta(days=1)).strftime('%d-%b-%y')

    lb = le_b if le_b is not None else 999999
    sb = se_b if se_b is not None else 999999
    
    curr_sl = float(df_sig['sl_price'].iloc[-1]) if pd.notna(df_sig['sl_price'].iloc[-1]) else 0

    if lb <= sb:
        return {"label": fmt_label(lb, "My Long Entry Id"), "date": fmt_date(le_dt), "type": "CB Long", "bars": lb, "sl": curr_sl}
    else:
        return {"label": fmt_label(sb, "My Short Entry Id"), "date": fmt_date(se_dt), "type": "CB Short", "bars": sb, "sl": curr_sl}


# ── CUSTOM SCORE & ADTV ────────────────────────────────────────────────────
def calc_custom_score(df, sector, current_price):
    score = 0
    reasons = [] # Array untuk menyimpan alasan poin
    
    c = df["Close"]
    v = df["Volume"]
    tv = c * v  
    
    tv_ma20 = tv.rolling(20).mean()
    v_ma20 = v.rolling(20).mean()
    
    adtv_1m = 0
    if len(tv_ma20) > 0 and pd.notna(tv_ma20.iloc[-1]):
        adtv_1m = tv_ma20.iloc[-1] / 1_000_000_000 
        
    # Menentukan strat_cat di dalam custom score agar bisa dipakai syarat poin
    strat_cat = get_strategy_category(sector, current_price, adtv_1m)
    
    # 1. Syarat ADTV > 1 Miliar (+1 Poin)
    if adtv_1m > 1.0:
        score += 1
        reasons.append("ADTV > 1M")
        
    # 2. Syarat Turnover > MA20 (+1 Poin)
    if tv.iloc[-1] > tv_ma20.iloc[-1]: 
        score += 1
        reasons.append("TV > MA20")
        
    # 3. Syarat Volume > MA20 (+1 Poin)
    if v.iloc[-1] > v_ma20.iloc[-1]: 
        score += 1
        reasons.append("Vol > MA20")
        
    # 4. Kategori Strategi Breakout & Volatility (+1 Poin)
    if strat_cat == "Breakout & Volatility":
        score += 1
        reasons.append("Cocok Channel Breakout")
        
    # 5. Syarat Ichimoku Cloud (Harga di atas awan Kumo) (+1 Poin)
    if len(df) >= 52:
        tenkan = (df["High"].rolling(9).max() + df["Low"].rolling(9).min()) / 2
        kijun = (df["High"].rolling(26).max() + df["Low"].rolling(26).min()) / 2
        span_a = ((tenkan + kijun) / 2).shift(26)
        span_b = ((df["High"].rolling(52).max() + df["Low"].rolling(52).min()) / 2).shift(26)
        cloud_top = pd.concat([span_a, span_b], axis=1).max(axis=1)
        
        if pd.notna(cloud_top.iloc[-1]) and current_price > cloud_top.iloc[-1]:
            score += 1  
            reasons.append("Harga > Kumo")
            
    # Gabungkan alasan menjadi string yang rapi
    reason_str = ", ".join(reasons) if reasons else "-"
            
    return score, round(adtv_1m, 2), strat_cat, reason_str
  
# ── TV SCORE ───────────────────────────────────────────────────────────────
def calc_tv(df):
    s, n = 0, 0
    # Abaikan return reason dari TV lama, tapi hitungannya dibiarkan jalan
    def add(v): nonlocal s, n; s += v; n += 1
    try:
        c = df["Close"]; h = df["High"]; l = df["Low"]; cn = float(c.iloc[-1])
        
        for p in [10, 20, 50, 100, 200]:
            sma = c.rolling(p).mean().iloc[-1]
            ema = c.ewm(span=p, adjust=False).mean().iloc[-1]
            if pd.notna(sma): add(1 if cn > sma else -1 if cn < sma else 0)
            if pd.notna(ema): add(1 if cn > ema else -1 if cn < ema else 0)

        tk = (h.rolling(9).max() + l.rolling(9).min()) / 2
        kj = (h.rolling(26).max() + l.rolling(26).min()) / 2
        sa = ((tk + kj) / 2).shift(26)
        sb2 = ((h.rolling(52).max() + l.rolling(52).min()) / 2).shift(26)
        
        tk_0, kj_0, sa_0, sb2_0 = tk.iloc[-1], kj.iloc[-1], sa.iloc[-1], sb2.iloc[-1]
        if pd.notna(sb2_0):
            if sa_0 > sb2_0 and kj_0 > sa_0 and tk_0 > kj_0 and cn > tk_0: add(1)
            elif sa_0 < sb2_0 and kj_0 < sa_0 and tk_0 < kj_0 and cn < tk_0: add(-1)
            else: add(0)

        import ta as _ta

        rsi = _ta.momentum.RSIIndicator(c, 14).rsi()
        r0, r1 = rsi.iloc[-1], rsi.iloc[-2]
        if pd.notna(r0):
            if r0 < 30 and r0 > r1: add(1)
            elif r0 > 70 and r0 < r1: add(-1)
            else: add(0)

        stoch = _ta.momentum.StochasticOscillator(h, l, c, window=14, smooth_window=3)
        k, d = stoch.stoch(), stoch.stoch_signal()
        k0, d0 = k.iloc[-1], d.iloc[-1]
        if pd.notna(k0) and pd.notna(d0):
            if k0 < 20 and d0 < 20 and k0 > d0: add(1)
            elif k0 > 80 and d0 > 80 and k0 < d0: add(-1)
            else: add(0)

        cci = _ta.trend.CCIIndicator(h, l, c, window=20).cci()
        c0, c1 = cci.iloc[-1], cci.iloc[-2]
        if pd.notna(c0):
            if c0 < -100 and c0 > c1: add(1)
            elif c0 > 100 and c0 < c1: add(-1)
            else: add(0)

        adxi = _ta.trend.ADXIndicator(h, l, c, 14)
        adx, pdi, mdi = adxi.adx(), adxi.adx_pos(), adxi.adx_neg()
        av0, av1 = adx.iloc[-1], adx.iloc[-2]
        if pd.notna(av0):
            if pdi.iloc[-1] > mdi.iloc[-1] and av0 > 20 and av0 > av1: add(1)
            elif pdi.iloc[-1] < mdi.iloc[-1] and av0 > 20 and av0 > av1: add(-1)
            else: add(0)

        ao = _ta.momentum.AwesomeOscillatorIndicator(h, l).awesome_oscillator()
        ao0, ao1, ao2 = ao.iloc[-1], ao.iloc[-2], ao.iloc[-3]
        if pd.notna(ao0):
            saucer_buy = ao0 > 0 and ao0 > ao1 and ao1 < ao2
            zero_cross_buy = ao0 > 0 and ao1 < 0
            saucer_sell = ao0 < 0 and ao0 < ao1 and ao1 > ao2
            zero_cross_sell = ao0 < 0 and ao1 > 0
            if saucer_buy or zero_cross_buy: add(1)
            elif saucer_sell or zero_cross_sell: add(-1)
            else: add(0)

        mom = c.diff(10)
        m0, m1 = mom.iloc[-1], mom.iloc[-2]
        if pd.notna(m0):
            if m0 > m1: add(1)
            elif m0 < m1: add(-1)
            else: add(0)

        macd = _ta.trend.MACD(c)
        m_line, m_sig = macd.macd(), macd.macd_signal()
        if pd.notna(m_line.iloc[-1]):
            if m_line.iloc[-1] > m_sig.iloc[-1]: add(1)
            elif m_line.iloc[-1] < m_sig.iloc[-1]: add(-1)
            else: add(0)

        wpr = _ta.momentum.WilliamsRIndicator(h, l, c, 14).williams_r()
        w0, w1 = wpr.iloc[-1], wpr.iloc[-2]
        if pd.notna(w0):
            if w0 < -80 and w0 > w1: add(1)
            elif w0 > -20 and w0 < w1: add(-1)
            else: add(0)

        uo = _ta.momentum.UltimateOscillator(h, l, c).ultimate_oscillator()
        u0 = uo.iloc[-1]
        if pd.notna(u0):
            if u0 > 70: add(1)
            elif u0 < 30: add(-1)
            else: add(0)

        fv = s / n if n > 0 else 0
        
        if -1.0 <= fv < -0.5: lbl = "Jual Kuat"
        elif -0.5 <= fv < -0.1: lbl = "Jual"
        elif -0.1 <= fv <= 0.1: lbl = "Netral"
        elif 0.1 < fv <= 0.5: lbl = "Beli"
        elif 0.5 < fv <= 1.0: lbl = "Beli Kuat"
        else: lbl = "Netral"
            
        return round(fv, 2), lbl, "-"
    except Exception as e:
        return 0.0, "Netral", "-"

# ── COMMODITY ──────────────────────────────────────────────────────────────
def fetch_commodities():
    print("\n📦 Commodities...")
    ctx = {}
    for name, cfg in COMMODITIES.items():
        for ticker in [cfg["t"], FALLBACK.get(name,"")]:
            if not ticker: continue
            try:
                raw = yf.download(ticker, period="90d", interval="1d",
                                  progress=False, auto_adjust=True)
                if raw is None or len(raw) < cfg["ma"]+5: continue
                if isinstance(raw.columns, pd.MultiIndex):
                    raw.columns = [c[0] for c in raw.columns]
                c  = raw["Close"]
                ma = float(c.rolling(cfg["ma"]).mean().iloc[-1])
                cl = float(c.iloc[-1])
                pr = float(c.iloc[-6]) if len(raw)>=6 else cl
                chg= round((cl-pr)/pr*100,2) if pr>0 else 0
                up = (cl<ma) if name=="DXY" else (cl>ma)
                ctx[name]={"up":up,"close":round(cl,2),"ma":round(ma,2),
                           "chg":chg,"s":cfg["s"],"ticker":ticker}
                print(f"  {'✅' if up else '⚠️'} {name:<12} {cl:>10.2f}  MA={ma:>10.2f}  {chg:>+6.2f}%")
                break
            except: continue
        if name not in ctx:
            ctx[name]={"up":None,"s":cfg["s"],"ticker":cfg["t"]}
            print(f"  ❌ {name}")
    return ctx

def comm_sector(sector, ctx):
    rel=[(n,d["up"]) for n,d in ctx.items()
         if d.get("up") is not None and ("ALL" in d["s"] or sector in d["s"])]
    if not rel: return {"pct":50.0,"summary":"-"}
    bull=sum(1 for _,u in rel if u)
    pct =round(bull/len(rel)*100,1)
    summ=" | ".join(f"{'✅' if u else '⚠️'}{n}" for n,u in rel[:5])
    return {"pct":pct,"summary":summ}

# ── ANALYZE TICKER ─────────────────────────────────────────────────────────
def analyze(ticker, sector, ctx):
    df = get_ohlcv(ticker)
    if df is None or len(df) < max(LENGTH, 52)+2:
        print(f"    [skip] {ticker}"); return None

    close_now = float(df["Close"].iloc[-1])
    tgl = df.index[-1].strftime("%d-%b-%y")

    # Menerima data strat_cat & list string score dari calc_custom_score
    custom_score, adtv, strat_cat, skor_reasons = calc_custom_score(df, sector, close_now)
    
    cb = calc_cb(df)
    tvs, tvl, _ = calc_tv(df)
    comm = comm_sector(sector, ctx)

    warning = " (⚠️ Sepi)" if adtv < 1.0 else ""
    if cb["type"] == "CB Long":
        if cb["bars"] <= 5: 
            action = f"BUY NOW{warning}"
        else: 
            action = f"HOLD{warning}"
    else:
        action = f"SELL / WAIT{warning}"

    return {
        "Ticker"                  : ticker,
        "Kategori Strategi"       : strat_cat,
        "Sektor"                  : sector,
        "Action"                  : action,
        "Harga"                   : int(close_now),
        "Batas Jual (SL)"         : int(cb["sl"]),
        "ADTV (M)"                : adtv,
        "Tgl Data"                : tgl,
        "Channel Breakout Signal" : cb["label"],
        "Tgl Channel Breakout"    : cb["date"],
        "Skor Tambahan"           : custom_score,
        "_type"                   : cb["type"],
        "_bars"                   : cb["bars"],
        "Skor TV"                 : tvs,
        "Rek TV"                  : tvl,
        "Alasan Rek TV"           : skor_reasons, 
        "Commodity Bullish %"     : comm["pct"],
        "Commodity Context"       : comm["summary"],
    }

# ── SECTOR ─────────────────────────────────────────────────────────────────
def run_sector(sector, tickers, ctx):
    print(f"\n📊 {sector}")
    rows=[]
    for i,t in enumerate(tickers,1):
        print(f"  [{i:>2}/{len(tickers)}] {t}...", end=" ", flush=True)
        r=analyze(t,sector,ctx)
        if r: rows.append(r); print(r["Channel Breakout Signal"])
        else: print("skip")
        time.sleep(0.3)
    if not rows: return pd.DataFrame()
    df=pd.DataFrame(rows)
    df["_le"]=(df["_type"]=="CB Long").astype(int)
    df=df.sort_values(["_le","_bars"],ascending=[False,True])
    df=df.drop(columns=["_type","_bars","_le"]).reset_index(drop=True)
    return df

# ── UPLOAD ─────────────────────────────────────────────────────────────────
def upload_sector(sector, df, ctx):
    ws=gsheet(sector)
    if not ws: return
    try:
        ws.clear()
        ts=datetime.now(WIB).strftime("%Y-%m-%d %H:%M WIB")
        cm=comm_sector(sector,ctx)
        ws.update("A1",[[f"📊 {sector} | Channel Breakout (Length={LENGTH}) | {ts}"]])
        ws.update("A2",[[f"Commodity Bullish: {cm['pct']}%"]])
        ws.update("A3",[[""]])
        avail=[c for c in DISPLAY_COLS if c in df.columns]
        set_with_dataframe(ws,df[avail],row=4,col=1,include_index=False)
        nc=len(avail)
        ws.format(f"A4:{chr(64+min(nc,26))}4",
                  {"textFormat":{"bold":True},
                   "backgroundColor":{"red":0.12,"green":0.20,"blue":0.50}})
        print(f"  ✅ {sector}: {len(df)} rows")
    except Exception as e: print(f"  ❌ {sector}: {e}")

def upload_summary(all_rows, ctx):
    print(f"\n📤 Summary → {SUMMARY_SHEET}")
    ws=gsheet(SUMMARY_SHEET)
    if not ws: return
    try:
        ws.clear()
        ts=datetime.now(WIB).strftime("%Y-%m-%d %H:%M WIB")
        ws.update("A1",[[
            f"🔍 IHSG Channel Breakout Screener v5.0 — {ts}","",
            f"CB Long = Breakout Resistance | CB Short = Breakdown Support | auto_adjust=False"]])

        crow=[["Commodity","Ticker","Close","MA","1W%","Status"]]
        for name,d in ctx.items():
            if d.get("close") is None:
                crow.append([name,d["ticker"],"-","-","-","❌"]); continue
            st=("✅ Weak(Bullish EM)" if (name=="DXY" and d["up"]) else
                "⚠️ Strong" if name=="DXY" else
                "✅ Bullish" if d["up"] else "⚠️ Bearish")
            crow.append([name,d["ticker"],d["close"],d["ma"],f"{d['chg']:+.2f}%",st])
        ws.update("A2",crow)

        data_row=len(crow)+4
        df=pd.DataFrame(all_rows)
        if df.empty: print("  ⚠️  No data"); return

        df["_le"]=(df.get("_type","")=="CB Long").astype(int)
        df["_b"]=pd.to_numeric(df.get("_bars",999999),errors="coerce").fillna(999999)
        
        df=df.sort_values(["_le","_b","Skor TV"],ascending=[False,True,False])
        df=df.drop(columns=["_le","_b","_type","_bars"],errors="ignore").reset_index(drop=True)

        avail=[c for c in DISPLAY_COLS if c in df.columns]
        set_with_dataframe(ws,df[avail],row=data_row,col=1,include_index=False)
        nc=len(avail)
        ws.format(f"A{data_row}:{chr(64+min(nc,26))}{data_row}",
                  {"textFormat":{"bold":True},
                   "backgroundColor":{"red":0.10,"green":0.35,"blue":0.22}})
        print(f"  ✅ {len(df)} rows")
    except Exception as e: print(f"  ❌ Summary: {e}")

# ── MAIN ───────────────────────────────────────────────────────────────────
def main():
    ts=datetime.now(WIB).strftime("%Y-%m-%d %H:%M WIB")
    print(f"\n{'═'*60}\n  IHSG Channel Breakout Screener v5.0 (Trial Mode)   {ts}")
    print(f"  Length={LENGTH} | {DOWNLOAD_DAYS}d history")
    print(f"{'═'*60}\n")

    ctx=fetch_commodities()
    all_rows=[]

    for sector,tickers in SECTORS.items():
        df_s=run_sector(sector,tickers,ctx)
        if df_s.empty: continue
        all_rows.extend(df_s.to_dict("records"))
        upload_sector(sector,df_s,ctx)
        time.sleep(2)

    if not all_rows: print("❌ No results"); sys.exit(1)
    upload_summary(all_rows,ctx)

    df_f=pd.DataFrame(all_rows)
    print(f"\n{'═'*60}")
    print(f"  Total           : {len(df_f)}")
    
    print(f"  CB Long         : {df_f['Channel Breakout Signal'].str.contains('My Long Entry Id',na=False).sum()}")
    print(f"  CB Short        : {df_f['Channel Breakout Signal'].str.contains('My Short Entry Id',na=False).sum()}")
    
    print(f"  Breakout HARI INI: {df_f['Channel Breakout Signal'].str.contains('hari ini',na=False).sum()}")
    print(f"{'═'*60}\n")

if __name__ == "__main__":
    main()
