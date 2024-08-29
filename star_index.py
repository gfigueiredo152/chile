import requests
import pandas as pd
from datetime import datetime

def API():
    Cod_API = pd.DataFrame({
        "name":['1_Imacec_IMACEC','2_Producción_de_bienes_IMACEC','3_Minería_IMACEC','4_Industria_IMACEC','5_Resto_de_bienes_IMACEC','6_Comercio_IMACEC','7_Servicios_IMACEC',
                '8_Imacec_a_costo_de_factores_IMACEC','10_Imacec_no_minero_IMACEC','1_IMCE:_Comercio_IMCE','2_IMCE:_Construcción_IMCE','3_IMCE:_Industria_IMCE',
                '4_IMCE:_Minería_IMCE','5_IMCE:_Total_IMCE','6_IMCE:__Sin_Minería_IMCE','1_Dólar_observado_CÂMBIO','1_Exportaciones_EXPORTAÇÕES','2_Minería_EXPORTAÇÕES',
                '3_Cobre_EXPORTAÇÕES','4_Cátodos_EXPORTAÇÕES','5_Concentrados_EXPORTAÇÕES','6_Hierro_EXPORTAÇÕES','7_Plata_EXPORTAÇÕES','8_Oro_EXPORTAÇÕES','9_Concentrado_de_molibdeno_EXPORTAÇÕES',
                '10_Carbonato_de_litio_EXPORTAÇÕES','21_Palta_EXPORTAÇÕES','25_Sector_silvícola_EXPORTAÇÕES','26_Pesca_extractiva_EXPORTAÇÕES','27_Industriales_EXPORTAÇÕES',
                '28_Alimentos_EXPORTAÇÕES','31_Salmón_EXPORTAÇÕES','40_Carne_de_ave_EXPORTAÇÕES','41_Carne_de_cerdo_EXPORTAÇÕES','42_Bebidas_y_tabaco_EXPORTAÇÕES','44_Vino_embotellado_EXPORTAÇÕES',
                '45_Vino_a_granel_y_otros_EXPORTAÇÕES','46_Forestal_y_muebles_de_madera_EXPORTAÇÕES','47_Madera_aserrada_EXPORTAÇÕES','49_Madera_perfilada_EXPORTAÇÕES',
                '51_Madera_contrachapada_EXPORTAÇÕES','52_Celulosa,_papel_y_otros_EXPORTAÇÕES','53_Celulosa_cruda_de_conífera_EXPORTAÇÕES','54_Celulosa_blanqueada_y_semiblanqueada_de_conífera_EXPORTAÇÕES',
                '55_Celulosa_blanqueada_y_semiblanqueada_de_eucaliptus_EXPORTAÇÕES','57_Productos_químicos_EXPORTAÇÕES','60_Nitrato_de_potasio_EXPORTAÇÕES','61_Abonos_EXPORTAÇÕES',
                '62_Oxido_de_molibdeno_EXPORTAÇÕES','64_Industria_metálica_básica_EXPORTAÇÕES','65_Ferromolibdeno_EXPORTAÇÕES','66_Alambre_de_cobre_EXPORTAÇÕES',
                '67_Productos_metálicos_maquinaria_y_equipos_EXPORTAÇÕES','68_Manufacturas_metálicas_EXPORTAÇÕES','69_Maquinaria_y_equipos_EXPORTAÇÕES','70_Material_de_transporte_EXPORTAÇÕES',
                '71_Otros_productos_industriales_EXPORTAÇÕES','1_Total_importaciones_de_bienes_(CIF)_Importações','2_Total_importaciones_de_bienes__(FOB)_Importações','3_Bienes_de_consumo_Importações',
                '4_Durables_Importações','5_Automóviles_Importações','10_Semidurables_Importações','11_Vestuario_Importações','12_Calzado_Importações','14_Carne_Importações','15_Otros_alimentos_Importações',
                '16_Bebidas_y_alcoholes_Importações','17_Gasolinas_Importações','18_Gas_licuado_Importações','21_Bienes_intermedios_Importações','22_Productos_energéticos_Importações','23_Petróleo_Importações',
                '24_Diésel_Importações','25_Carbón_mineral_Importações','26_Gas_natural_licuado_Importações','27_Gas_natural_gaseoso_Importações','33_Partes_y_piezas_de_maquinaria_para_la_minería_y_la_construcción_Importações',
                '34_Partes_y_piezas_de_otras_maquinarias_y_equipos_Importações','42_Camiones_y_vehículos_de_carga_Importações','43_Buses_Importações','44_Otros_vehículos_de_transporte_Importações',
                '45_Maquinaria_para_la_minería_y_la_construcción_Importações'],
        "cod_API":['F032.IMC.IND.Z.Z.EP18.Z.Z.0.M','F032.IMC.IND.Z.Z.EP18.PB.Z.0.M','F032.IMC.IND.Z.Z.EP18.03.Z.0.M','F032.IMC.IND.Z.Z.EP18.04.Z.0.M','F032.IMC.IND.Z.Z.EP18.RB.Z.0.M',
                   'F032.IMC.IND.Z.Z.EP18.COM.Z.0.M','F032.IMC.IND.Z.Z.EP18.SERV.Z.0.M','F032.ICF.IND.Z.Z.EP18.Z.Z.0.M','F032.IMC.IND.Z.Z.EP18.N03.Z.0.M','G089.IME.IND.A1.M',
                   'G089.IME.IND.A3.M','G089.IME.IND.A2.M','G089.IME.IND.A4.M','G089.IME.IND.A0.M','G089.IME.IND.A04.M','F073.TCO.PRE.Z.D','F068.B1.FLU.Z.0.C.N.Z.Z.Z.Z.6.0.M',
                   'F068.B1.FLU.A.0.C.N.Z.Z.Z.Z.6.0.M','F068.B1.FLU.A1.0.C.N.Z.Z.Z.Z.6.0.M','F068.B1.FLU.A2.0.C.N.Z.Z.Z.Z.6.0.M','F068.B1.FLU.A3.0.C.N.Z.Z.Z.Z.6.0.M',
                   'F068.B1.FLU.A4.0.C.N.Z.Z.Z.Z.6.0.M','F068.B1.FLU.A5.0.C.N.Z.Z.Z.Z.6.0.M','F068.B1.FLU.A6.0.C.N.Z.Z.Z.Z.6.0.M','F068.B1.FLU.A7.0.C.N.Z.Z.Z.Z.6.0.M',
                   'F068.B1.FLU.A8.0.C.N.Z.Z.Z.Z.6.0.M','F068.B1.FLU.B18.0.C.N.Z.Z.Z.Z.6.0.M','F068.B1.FLU.B3.0.C.N.Z.Z.Z.Z.6.0.M','F068.B1.FLU.B4.0.C.N.Z.Z.Z.Z.6.0.M',
                   'F068.B1.FLU.C.0.C.N.Z.Z.Z.Z.6.0.M','F068.B1.FLU.C1.0.C.N.Z.Z.Z.Z.6.0.M','F068.B1.FLU.C13.0.C.N.Z.Z.Z.Z.6.0.M','F068.B1.FLU.C1C.0.C.N.Z.Z.Z.Z.6.0.M',
                   'F068.B1.FLU.C1D.0.C.N.Z.Z.Z.Z.6.0.M','F068.B1.FLU.C2.0.C.N.Z.Z.Z.Z.6.0.M','F068.B1.FLU.C22.0.C.N.Z.Z.Z.Z.6.0.M','F068.B1.FLU.C23.0.C.N.Z.Z.Z.Z.6.0.M',
                   'F068.B1.FLU.C3.0.C.N.Z.Z.Z.Z.6.0.M','F068.B1.FLU.C31.0.C.N.Z.Z.Z.Z.6.0.M','F068.B1.FLU.C33.0.C.N.Z.Z.Z.Z.6.0.M','F068.B1.FLU.C35.0.C.N.Z.Z.Z.Z.6.0.M',
                   'F068.B1.FLU.C4.0.C.N.Z.Z.Z.Z.6.0.M','F068.B1.FLU.C45.0.C.N.Z.Z.Z.Z.6.0.M','F068.B1.FLU.C42.0.C.N.Z.Z.Z.Z.6.0.M','F068.B1.FLU.C43.0.C.N.Z.Z.Z.Z.6.0.M',
                   'F068.B1.FLU.C5.0.C.N.Z.Z.Z.Z.6.0.M','F068.B1.FLU.C53.0.C.N.Z.Z.Z.Z.6.0.M','F068.B1.FLU.C53A.0.C.N.Z.Z.Z.Z.6.0.M','F068.B1.FLU.C55.0.C.N.Z.Z.Z.Z.6.0.M',
                   'F068.B1.FLU.C6.0.C.N.Z.Z.Z.Z.6.0.M','F068.B1.FLU.C61.0.C.N.Z.Z.Z.Z.6.0.M','F068.B1.FLU.C62.0.C.N.Z.Z.Z.Z.6.0.M','F068.B1.FLU.C7.0.C.N.Z.Z.Z.Z.6.0.M',
                   'F068.B1.FLU.C71.0.C.N.Z.Z.Z.Z.6.0.M','F068.B1.FLU.C72.0.C.N.Z.Z.Z.Z.6.0.M','F068.B1.FLU.C73.0.C.N.Z.Z.Z.Z.6.0.M','F068.B1.FLU.C9.0.C.N.Z.Z.Z.Z.6.0.M',
                   'F068.B1.FLU.Z.0.M.N.0.Z.Z.Z.6.0.M','F068.B1.FLU.Z.0.D.N.0.T.Z.Z.6.0.M','F068.B1.FLU.Z.0.M.N.A.Z.Z.Z.6.0.M','F068.B1.FLU.Z.0.M.N.B.Z.Z.Z.6.0.M',
                   'F068.B1.FLU.C731.0.M.N.B.Z.Z.Z.6.0.M','F068.B1.FLU.Z.0.M.N.C.Z.Z.Z.6.0.M','F068.B1.FLU.C91.0.M.N.C.Z.Z.Z.6.0.M','F068.B1.FLU.C92.0.M.N.C.Z.Z.Z.6.0.M',
                   'F068.B1.FLU.C1F.0.M.N.D.Z.Z.Z.6.0.M','F068.B1.FLU.C1G.0.M.N.D.Z.Z.Z.6.0.M','F068.B1.FLU.C24A.0.M.N.D.Z.Z.Z.6.0.M','F068.B1.FLU.PA.0.M.N.D.Z.Z.Z.6.0.M',
                   'F068.B1.FLU.PB.0.M.N.D.Z.Z.Z.6.0.M','F068.B1.FLU.Z.0.M.N.E.Z.Z.Z.6.0.M','F068.B1.FLU.Z.0.M.N.F.Z.Z.Z.6.0.M','F068.B1.FLU.Z.0.M.N.G.Z.Z.Z.6.0.M','F068.B1.FLU.P4.0.M.N.G.Z.Z.Z.6.0.M',
                   'F068.B1.FLU.P5.0.M.N.G.Z.Z.Z.6.0.M','F068.B1.FLU.P6.0.M.N.G.Z.Z.Z.6.0.M','F068.B1.FLU.P7.0.M.N.G.Z.Z.Z.6.0.M','F068.B1.FLU.C751.0.M.N.J.Z.Z.Z.6.0.M','F068.B1.FLU.C752.0.M.N.J.Z.Z.Z.6.0.M',
                   'F068.B1.FLU.C732.0.M.N.K.Z.Z.Z.6.0.M','F068.B1.FLU.C733.0.M.N.K.Z.Z.Z.6.0.M','F068.B1.FLU.C734.0.M.N.K.Z.Z.Z.6.0.M','F068.B1.FLU.C753.0.M.N.K.Z.Z.Z.6.0.M']


    })
    return Cod_API



def site():
    now = datetime.now()
    ATL_current_year = now.year
    ATL_current_month = now.month
    
    # Calculando o mês anterior com lógica para lidar com janeiro
    if ATL_current_month == 1:
        ANT_current_month = 12
        ATL_current_year -= 1
    else:
        ANT_current_month = ATL_current_month - 1

    urls = pd.DataFrame({
        "name": ['PRECIO_DEL_COBRE_NOMINAL_REAL','PRECIOS_PROMEDIOS_DEL_MOLIBDENO','PRECIOS_DEL_ORO','PRECIOS_DE_LA_PLATA','PRODUCCION_CHILENA_DE_COBRE_POR_PRODUCTOS','PRODUCCION_CHILENA_DE_ORO_PLATA','EMBARQUES_FISICOS_DE_EXPORTACION_DE_COBRE_MOLIBDENO'],
        "web": [
            f'https://boletin.cochilco.cl/productos/boletin.asp?anio={ATL_current_year}&mes={ANT_current_month:02d}&tabla=tabla1',
            f'https://boletin.cochilco.cl/productos/boletin.asp?anio={ATL_current_year}&mes={ATL_current_month:02d}&tabla=tabla9',
            f'https://boletin.cochilco.cl/productos/boletin.asp?anio={ATL_current_year}&mes={ATL_current_month:02d}&tabla=tabla7_1',
            f'https://boletin.cochilco.cl/productos/boletin.asp?anio={ATL_current_year}&mes={ATL_current_month:02d}&tabla=tabla8_1',
            f'https://boletin.cochilco.cl/productos/boletin.asp?anio={ATL_current_year}&mes={ATL_current_month:02d}&tabla=tabla21',
            f'https://boletin.cochilco.cl/productos/boletin.asp?anio={ATL_current_year}&mes={ATL_current_month:02d}&tabla=tabla24',
            f'https://boletin.cochilco.cl/productos/boletin.asp?anio={ATL_current_year}&mes={ATL_current_month:02d}&tabla=tabla26'
        ],
        "tipo": ['tabela_1','tabela_9','tabela_7.1','tabela_8.1','tabela_21','tabela_24','tabela_26']


    })


    results = []
    for url in urls['web']:
        response = requests.get(url)
        if response.status_code == 200:
            status = "Success"
        else:
            status = "Failed"
            print(f"Failed to fetch data from {url}. Status code: {response.status_code}")

        results.append(status)

    return results, urls

