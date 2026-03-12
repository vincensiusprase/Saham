"""
IHSG Channel Breakout Screener v5.0
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
LENGTH          = 5  # Menggantikan ATR_LENGTH & FACTOR
DOWNLOAD_DAYS   = 365
WIB             = timezone(timedelta(hours=7))

# ── SECTORS (TRIAL: Max 10 Tickers / Sector) ───────────────────────────────
SECTORS = {
        "IDXINDUST": [
        "AMFG.JK", "AMIN.JK", "APII.JK", "ARKA.JK", "ARNA.JK", "ASGR.JK", "ASII.JK", "BHIT.JK", "BINO.JK", "BLUE.JK", 
        "BNBR.JK", "CAKK.JK", "CCSI.JK", "CRSN.JK", "CTTH.JK", "DYAN.JK", "FOLK.JK", "GPSO.JK", "HEXA.JK", "HOPE.JK", 
        "HYGN.JK", "IBFN.JK", "ICON.JK", "IKAI.JK", "IKBI.JK", "IMPC.JK", "INDX.JK", "INTA.JK", "JECC.JK", "JTPE.JK", 
        "KBLI.JK", "KBLM.JK", "KIAS.JK", "KING.JK", "KOBX.JK", "KOIN.JK", "KONI.JK", "KUAS.JK", "LABA.JK", "LION.JK", "MARK.JK", 
        "MDRN.JK", "MFMI.JK", "MHKI.JK", "MLIA.JK", "MUTU.JK", "NAIK.JK", "NTBK.JK", "PADA.JK", "PIPA.JK", "PTMP.JK", 
        "SCCO.JK", "SINI.JK", "SKRN.JK", "SMIL.JK", "SOSS.JK", "SPTO.JK", "TIRA.JK", "TOTO.JK", "TRIL.JK", "UNTR.JK", 
        "VISI.JK", "VOKS.JK", "ZBRA.JK"
    ],
    "IDXNONCYC": [
        "AALI.JK", "ADES.JK", "AGAR.JK", "AISA.JK", "ALTO.JK", "AMMS.JK", "AMRT.JK", "ANDI.JK", "ANJT.JK", "ASHA.JK", 
        "AYAM.JK", "BEEF.JK", "BEER.JK", "BISI.JK", "BOBA.JK", "BRRC.JK", "BTEK.JK", "BUAH.JK", "BUDI.JK", "BWPT.JK", 
        "CAMP.JK", "CBUT.JK", "CEKA.JK", "CLEO.JK", "CMRY.JK", "COCO.JK", "CPIN.JK", "CRAB.JK", "CPRO.JK", "CSRA.JK", 
        "DAYA.JK", "DEWI.JK", "DLTA.JK", "DMND.JK", "DPUM.JK", "DSFI.JK", "DSNG.JK", "ENZO.JK", "EPMT.JK", "EURO.JK", "FAPA.JK", 
        "FISH.JK", "FLMC.JK", "FOOD.JK", "FORE.JK", "GGRM.JK", "GOLL.JK", "GOOD.JK", "GRPM.JK", "GULA.JK", "GUNA.JK", "GZCO.JK", 
        "HERO.JK", "HMSD.JK", "HMSP.JK", "HOKI.JK","IBOS.JK", "ICBP.JK", "IKAN.JK", "INDF.JK", "IPPE.JK", "ISEA.JK", "ITIC.JK", "JARR.JK", 
        "JAWA.JK", "JPFA.JK", "KEJU.JK", "KINO.JK", "KMDS.JK", "LAPD.JK", "LSIP.JK", "MAGP.JK", "MAIN.JK", "MAXI.JK", 
        "MBTO.JK", "MGRO.JK", "MIDI.JK", "MKTR.JK", "MLBI.JK", "MLPL.JK", "MPPA.JK", "MRAT.JK", "MSJA.JK", "MYOR.JK","NANO.JK", 
        "NASI.JK", "NAYZ.JK", "NEST.JK", "NSSS.JK", "OILS.JK", "PCAR.JK", "PGUN.JK", "PMMP.JK", "PNGO.JK", "PSDN.JK", "PSGO.JK", 
        "PTPS.JK", "RANC.JK", "RLCO.JK", "ROTI.JK", "SDPC.JK", "SGRO.JK", "SIMP.JK", "SIPD.JK", "SKBM.JK", "SKLT.JK", 
        "SMAR.JK","SOUL.JK", "SSMS.JK", "STAA.JK", "STRK.JK", "STTP.JK", "TAPG.JK", "TAYS.JK", "TBLA.JK", "TCID.JK", "TGKA.JK", 
        "TGUK.JK", "TLDN.JK", "TRGU.JK", "UCID.JK", "UDNG.JK", "ULTJ.JK", "UNSP.JK", "UNVR.JK", "VICI.JK", "WAPO.JK", 
        "WICO.JK", "WIIM.JK", "WINE.JK", "WMPP.JK", "WMUU.JK", "YUPI.JK"
    ],
    "IDXFINANCE": [
        "ABDA.JK", "ADMF.JK", "AGRO.JK", "AGRS.JK", "AHAP.JK", "AMAG.JK", "AMAR.JK", "AMOR.JK", "APIC.JK", "ARTO.JK", 
        "ASBI.JK", "ASDM.JK", "ASJT.JK", "ASMI.JK", "ASRM.JK", "BABP.JK", "BACA.JK", "BANK.JK", "BBCA.JK", "BBHI.JK", 
        "BBKP.JK", "BBLD.JK", "BBMD.JK", "BBNI.JK", "BBRI.JK", "BBSI.JK", "BBTN.JK", "BBYB.JK", "BCAP.JK", "BCIC.JK", 
        "BDMN.JK", "BEKS.JK", "BFIN.JK", "BGTG.JK", "BHAT.JK", "BHIT.JK", "BINA.JK", "BJBR.JK", "BJTM.JK", "BKSW.JK", "BMAS.JK", 
        "BMRI.JK", "BNBA.JK", "BNGA.JK", "BNII.JK", "BNLI.JK", "BPFI.JK", "BPII.JK", "BRIS.JK", "BSIM.JK", "BSWD.JK", 
        "BTPN.JK", "BTPS.JK", "BVIC.JK", "CASA.JK", "CFIN.JK", "COIN.JK", "DEFI.JK", "DNAR.JK", "DNET.JK", "FUJI.JK", "GSMF.JK", "HBAT.JK", 
        "HDFA.JK", "INPC.JK", "IPAC.JK", "JMAS.JK","KIJA.JK", "LIFE.JK", "LPGI.JK", "LPPS.JK", "MASB.JK", "MAYA.JK", "MCOR.JK", "MEGA.JK", 
        "MREI.JK", "MSIE.JK", "MTWI.JK", "NICK.JK", "NISP.JK", "NOBU.JK", "OCAP.JK", "PADI.JK", "PALM.JK", "PANS.JK", "PEGE.JK", 
        "PLAS.JK", "PNBN.JK", "PNBS.JK", "PNIN.JK", "PNLF.JK", "POLA.JK", "POOL.JK", "RELF.JK","RELI.JK", "SDRA.JK", "SFAN.JK", 
        "SMMA.JK", "SRTG.JK", "STAR.JK", "SUPA.JK", "TIFA.JK", "TRIM.JK", "TRUS.JK", "TUGU.JK", "VICO.JK", "VINS.JK", 
        "VRNA.JK", "VTNY.JK", "WIDI.JK", "WOMF.JK", "YOII.JK", "YULE.JK"
    ],
    "IDXCYCLIC": [
        "ABBA.JK", "ACES.JK", "ACRO.JK", "AEGS.JK", "AKKU.JK", "ARGO.JK", "ARTA.JK", "ASLC.JK", "AUTO.JK", "BABY.JK", 
        "BAIK.JK", "BATA.JK", "BAUT.JK", "BAYU.JK", "BELL.JK", "BIKE.JK", "BIMA.JK", "BLTZ.JK", "BMBL.JK", "BMTR.JK", "BOGA.JK", 
        "BOLA.JK", "BOLT.JK", "BRAM.JK", "BUVA.JK", "CARS.JK", "CBMF.JK", "CINT.JK", "CLAY.JK", "CNMA.JK", "CNTX.JK", 
        "CSAP.JK", "CSMI.JK", "DEPO.JK", "DFAM.JK", "DIGI.JK", "DOOH.JK", "DOSS.JK", "DRMA.JK", "DUCK.JK", "EAST.JK", 
        "ECII.JK", "ENAK.JK", "ERAA.JK", "ERAL.JK", "ERTX.JK", "ESTA.JK", "ESTI.JK", "FAST.JK", "FILM.JK", "FITT.JK", 
        "FORU.JK", "FUTR.JK", "GDYR.JK", "GEMA.JK", "GJTL.JK", "GLOB.JK", "GOLF.JK", "GRPH.JK", "GWSA.JK","HAJJ.JK", "HOME.JK", 
        "HOTL.JK", "HRME.JK", "HRTA.JK", "IDEA.JK", "IIKP.JK", "IMAS.JK", "INDR.JK", "INDS.JK", "INOV.JK", "IPTV.JK", "ISAP.JK", 
        "JGLE.JK", "JIHD.JK", "JSPT.JK", "KAQI.JK", "KDTN.JK", "KICI.JK", "KLIN.JK", "KOTA.JK", "KPIG.JK", "LFLO.JK", "LIVE.JK", "LMAX.JK", 
        "LMPI.JK", "LPIN.JK", "LPPF.JK", "LUCY.JK", "MABA.JK", "MAPA.JK", "MAPB.JK", "MAPI.JK", "MARI.JK", "MDIA.JK", "MDIY.JK", "MEJA.JK", 
        "MERI.JK", "MGNA.JK", "MGLV.JK", "MICE.JK", "MINA.JK", "MNCN.JK", "MPMX.JK", "MSIN.JK", "MSKY.JK", "MYTX.JK", "NATO.JK", 
        "NETV.JK", "NUSA.JK", "OLIV.JK", "PANR.JK", "PART.JK", "PBRX.JK", "PDES.JK", "PGLI.JK", "PJAA.JK", "PLAN.JK","PMJS.JK", "PMUI.JK", 
        "PNSE.JK", "POLU.JK", "POLY.JK", "PSKT.JK", "PTSP.JK", "PZZA.JK", "RAAM.JK", "RAFI.JK", "RALS.JK", "RICY.JK", 
        "SBAT.JK", "SCMA.JK", "SCNP.JK", "SHID.JK", "SLIS.JK", "SMSM.JK", "SNLK.JK","SOFA.JK" "SONA.JK", "SOTS.JK", 
       "SPRE.JK", "SRIL.JK", "SSTM.JK", "SWID.JK", "TELE.JK", "TFCO.JK", "TMPO.JK", "TOOL.JK", "TOYS.JK", "TRIO.JK", 
       "TRIS.JK", "TYRE.JK",  "UFOE.JK", "UNIT.JK", "UNTD.JK", "VERN.JK", "VIVA.JK", "VKTR.JK", "WOOD.JK", "YELO.JK", 
       "ZATA.JK", "ZONE.JK"
    ],
    "IDXTECHNO": [
        "AREA.JK", "ATIC.JK", "AWAN.JK", "AXIO.JK", "BELI.JK", "BUKA.JK", "CASH.JK", "CHIP.JK", "CYBR.JK", "DCII.JK", 
        "DIVA.JK", "DMMX.JK", "EDGE.JK", "ELIT.JK", "EMTK.JK", "ENVY.JK", "GLVA.JK", "GOTO.JK", "HDIT.JK", "IOTF.JK", 
        "IRSX.JK", "JATI.JK", "KIOS.JK", "KREN.JK", "LMAS.JK", "LUCK.JK", "MCAS.JK", "MLPT.JK", "MPIX.JK", "MSTI.JK", 
        "MTDL.JK", "NFCX.JK", "NINE.JK", "PGJO.JK", "PTSN.JK", "RUNS.JK", "SKYB.JK", "TECH.JK", "TFAS.JK", "TOSK.JK", 
        "TRON.JK", "UVCR.JK", "WGSH.JK", "WIFI.JK", "WIRG.JK", "ZYRX.JK"
    ],
    "IDXBASIC": [
        "ADMG.JK", "AGII.JK", "AKPI.JK", "ALDO.JK", "ALKA.JK", "ALMI.JK", "AMMN.JK", "ANTM.JK", "APLI.JK", "ARCI.JK", 
        "ASPR.JK", "AVIA.JK", "AYLS.JK", "BAJA.JK", "BATR.JK", "BEBS.JK", "BLES.JK", "BMSR.JK", "BRMS.JK", "BRNA.JK", 
        "BRPT.JK", "BTON.JK", "CHEM.JK", "CITA.JK", "CLPI.JK", "CMNT.JK", "CTBN.JK", "DAAZ.JK", "DGWG.JK", "DKFT.JK", 
        "DPNS.JK", "EKAD.JK", "EMAS.JK", "EPAC.JK", "ESIP.JK", "ESSA.JK", "ETWA.JK", "FASW.JK", "FPNI.JK", "FWCT.JK", 
        "GDST.JK", "GGRP.JK", "HKMU.JK", "IFII.JK", "IFSH.JK", "IGAR.JK", "INAI.JK", "INCF.JK", "INCI.JK", "INCO.JK", 
        "INKP.JK", "INRU.JK", "INTD.JK", "INTP.JK", "IPOL.JK", "ISSP.JK", "KAYU.JK", "KBRI.JK", "KDSI.JK", "KKES.JK", 
        "KMTR.JK", "KRAS.JK", "LMSH.JK", "LTLS.JK", "MBMA.JK", "MDKA.JK", "MDKI.JK", "MINE.JK", "MOLI.JK", "NCKL.JK", 
        "NICE.JK", "NICL.JK", "NIKL.JK", "NPGF.JK", "OBMD.JK", "OKAS.JK", "OPMS.JK", "PACK.JK", "PBID.JK", "PDPP.JK", 
        "PICO.JK", "PPRI.JK", "PSAB.JK", "PTMR.JK", "PURE.JK", "SAMF.JK", "SBMA.JK", "SIMA.JK", "SMBR.JK", "SMCB.JK", 
        "SMGA.JK", "SMGR.JK", "SMKL.JK", "SMLE.JK", "SOLA.JK", "SPMA.JK", "SQMI.JK", "SRSN.JK", "SULI.JK", "SWAT.JK", 
        "TALF.JK", "TBMS.JK", "TDPM.JK", "TINS.JK", "TIRT.JK", "TKIM.JK", "TPIA.JK", "TRST.JK", "UNIC.JK", "WSBP.JK", 
        "WTON.JK", "YPAS.JK", "ZINC.JK"
    ],
    "IDXENERGY": [
        "AADI.JK", "ABMM.JK", "ADMR.JK", "ADRO.JK", "AIMS.JK", "AKRA.JK", "ALII.JK", "APEX.JK", "ARII.JK", "ARTI.JK", 
        "ATLA.JK", "BBRM.JK", "BESS.JK", "BIPI.JK", "BOAT.JK", "BOSS.JK", "BSML.JK", "BSSR.JK", "BULL.JK", "BUMI.JK", 
        "BYAN.JK", "CANI.JK", "CBRE.JK", "CGAS.JK", "CNKO.JK", "COAL.JK", "CUAN.JK", "DEWA.JK", "DOID.JK", "DSSA.JK", 
        "DWGL.JK", "ELSA.JK", "ENRG.JK", "FIRE.JK", "GEMS.JK", "GTBO.JK", "GTSI.JK", "HILL.JK", "HITS.JK", "HRUM.JK", 
        "HUMI.JK", "IATA.JK", "INDY.JK", "INPS.JK", "ITMA.JK", "ITMG.JK", "JSKY.JK", "KKGI.JK", "KOPI.JK", "LEAD.JK", 
        "MAHA.JK", "MBAP.JK", "MBSS.JK", "MCOL.JK", "MEDC.JK", "MKAP.JK", "MTFN.JK", "MYOH.JK", "PGAS.JK", "PKPK.JK", 
        "PSAT.JK", "PSSI.JK", "PTBA.JK", "PTIS.JK", "PTRO.JK", "RAJA.JK", "RATU.JK", "RGAS.JK", "RIGS.JK", "RMKE.JK", 
        "RMKO.JK", "RUIS.JK", "SEMA.JK", "SGER.JK", "SHIP.JK", "SICO.JK", "SMMT.JK", "SMRU.JK", "SOCI.JK", "SUGI.JK", 
        "SUNI.JK", "SURE.JK", "TAMU.JK", "TCPI.JK", "TEBE.JK", "TOBA.JK", "TPMA.JK", "TRAM.JK", "UNIQ.JK", "WINS.JK", 
        "WOWS.JK"
    ],
    "IDXHEALTH": [
        "BMHS.JK", "CARE.JK", "CHEK.JK", "DGNS.JK", "DKHH.JK", "DVLA.JK", "HALO.JK", "HEAL.JK", "IKPM.JK", "INAF.JK", 
        "IRRA.JK", "KAEF.JK", "KLBF.JK", "LABS.JK", "MDLA.JK", "MEDS.JK", "MERK.JK", "MIKA.JK", "MMIX.JK", "MTMH.JK", 
        "OBAT.JK", "OMED.JK", "PEHA.JK", "PEVE.JK", "PRDA.JK", "PRAY.JK", "PRIM.JK", "PYFA.JK", "RSCH.JK", "RSGK.JK", 
        "SAME.JK", "SCPI.JK", "SIDO.JK", "SILO.JK", "SOHO.JK", "SRAJ.JK", "SURI.JK", "TSPC.JK"
    ],
    "IDXINFRA": [
        "ACST.JK", "ADHI.JK", "ARKO.JK", "ASLI.JK", "BALI.JK", "BDKR.JK", "BREN.JK", "BTEL.JK", "BUKK.JK", "CASS.JK", 
        "CDIA.JK", "CENT.JK", "CMNP.JK", "DATA.JK", "DGIK.JK", "EXCL.JK", "GHON.JK", "GMFI.JK", "GOLD.JK", "HADE.JK", 
        "HGII.JK", "IBST.JK", "IDPR.JK", "INET.JK", "IPCC.JK", "IPCM.JK", "ISAT.JK", "JAST.JK", "JKON.JK", "JSMR.JK", 
        "KARW.JK", "KBLV.JK", "KEEN.JK", "KETR.JK", "KOKA.JK", "KRYA.JK", "LCKM.JK", "LINK.JK", "META.JK", "MORA.JK", 
        "MPOW.JK", "MTEL.JK", "MTPS.JK", "MTRA.JK", "NRCA.JK", "OASA.JK", "PBSA.JK", "PGEO.JK", "PORT.JK", "POWR.JK", 
        "PPRE.JK", "PTDU.JK", "PTPP.JK", "PTPW.JK", "RONY.JK", "SMKM.JK","SSIA.JK", "SUPR.JK", "TAMA.JK", "TBIG.JK", 
        "TGRA.JK", "TLKM.JK", "TOPS.JK", "TOTL.JK", "TOWR.JK", "WEGE.JK", "WIKA.JK", "WSKT.JK"
    ],
    "IDXPROPERT": [
        "ADCP.JK", "AMAN.JK", "APLN.JK", "ARMY.JK", "ASPI.JK", "ASRI.JK", "ATAP.JK", "BAPA.JK", "BAPI.JK", "BBSS.JK", 
        "BCIP.JK", "BEST.JK", "BIKA.JK", "BIPP.JK", "BKDP.JK", "BKSL.JK", "BSBK.JK", "BSDE.JK", "CBDK.JK", "CBPE.JK", 
        "CITY.JK", "COWL.JK", "CPRI.JK", "CSIS.JK", "CTRA.JK", "DADA.JK", "DART.JK", "DILD.JK", "DMAS.JK", "DUTI.JK", 
        "ELTY.JK", "EMDE.JK", "FMII.JK", "GAMA.JK", "GMTD.JK", "GPRA.JK", "GRIA.JK", "HOMI.JK", "INDO.JK", "INPP.JK", 
        "JRPT.JK", "KBAG.JK", "KLJA.JK", "KOCI.JK", "KSIX.JK", "LAND.JK", "LCGP.JK", "LPCK.JK", "LPKR.JK", "LPLI.JK", "MANG.JK", 
        "MDLN.JK", "MKPI.JK", "MMLP.JK", "MPRO.JK", "MTLA.JK", "MTSM.JK", "NASA.JK", "NIRO.JK", "NZIA.JK", "OMRE.JK", 
        "PAMG.JK", "PANI.JK", "PLIN.JK", "POLI.JK", "POLL.JK", "POSA.JK", "PPRO.JK", "PUDP.JK", "PURI.JK", "PWON.JK", 
        "RBMS.JK", "RDTX.JK", "REAL.JK", "RIMO.JK", "RISE.JK", "ROCK.JK", "RODA.JK", "SAGE.JK", "SATU.JK", "SMDM.JK", 
        "SMRA.JK", "TARA.JK", "TRIN.JK", "TRUE.JK", "UANG.JK", "URBN.JK", "VAST.JK", "WINR.JK"
    ],
    "IDXTRANS": [
        "AKSI.JK", "ASSA.JK", "BIRD.JK", "BLOG.JK", "BLTA.JK", "BPTR.JK", "CMPP.JK", "DEAL.JK", "ELPI.JK", "GIAA.JK", 
        "GTRA.JK", "HAIS.JK", "HATM.JK", "HELI.JK", "IMJS.JK", "JAYA.JK", "KJEN.JK", "KLAS.JK", "LAJU.JK", "LOPI.JK", 
        "LRNA.JK", "MIRA.JK", "MITI.JK", "MPXL.JK", "NELY.JK", "PJHB.JK", "PPGL.JK", "PURA.JK", "RCCC.JK", "SAFE.JK", "SAPX.JK", 
        "SDMU.JK", "SMDR.JK", "TAXI.JK", "TMAS.JK", "TNCA.JK", "TRJA.JK", "TRUK.JK", "WEHA.JK"
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
    end   = datetime.now(WIB) + timedelta(days=1)
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

# ── CHANNEL BREAKOUT ────────────────────
def calc_cb(df):
    # Batas atas dan bawah tetap menggunakan High dan Low masa lalu (Classic Donchian)
    up   = df["High"].rolling(LENGTH).max().shift(1)
    down = df["Low"].rolling(LENGTH).min().shift(1)

    # FIX: Gunakan df["Close"] untuk trigger entry. 
    # Breakout hanya valid jika harga "Ditutup" di luar channel.
    # Ini 99% akan menghilangkan false signal akibat wick/jarum error di yfinance.
    le_condition = df["Close"] > up
    se_condition = df["Close"] < down

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
        ago = "Bar Terakhir (0)" if bars == 0 else f"{bars} bar lalu"
        return f"{lbl} ({ago})"

    def fmt_date(dt):
        if dt is None: return "-"
        return (pd.Timestamp(dt) + pd.Timedelta(days=0)).strftime('%d-%b-%y')

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
