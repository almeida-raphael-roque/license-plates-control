SELECT DISTINCT
    coalesce(iv.BOARD,it.BOARD,itt.BOARD) as "placa",
    coalesce(iv.chassi,it.chassi,itt.chassi) as "chassi",
    coalesce(iv.id,it.id,itt.id) as "id_placa",
    coalesce(irsc.ID_VEHICLE) as "id_veiculo",
    coalesce(irsct.ID_TRAILER) as "id_carroceria",
    ir.id as "Matricula",
    irs.id as "Conjunto",
    cata.fantasia as "Unidade",
    v.DESCRICAO as "consultor",
    iss.description as "Status",
    cat.nome as "Cliente",
    CAST(irs.data_registration AS DATE) as "data_registro",
    cast(coalesce(irs.DATE_INITAL_EFFECT,date_add('day', -364, irs.DATE_FINAL_EFFECT), irs.date_activation) as date) as "data_ativacao",
    user_name.suporte as "Suporte",
    current_date AS "data_filtro",
    'Segtruck' as empresa,
    irsc.id as "coverage",
    b.description as "beneficio",
    c.DESCRIPTION as "categoria",
    tc.DESCRIPTION as "tipo_categoria",
    cast(coalesce(irsc.date_initial_effect,date_add('day', -364, irsc.date_final_effect)) as date) as "data_ativacao_beneficio",
    isss.description as "status_beneficio",
    CAST('1900-01-01 00:00:00' AS TIMESTAMP) 
    + (irsc.UPDATED_AT - 599266080000000000) / 864000000000 * INTERVAL '1' DAY AS "data_atualizacao",
    v.ativo as "consultor_ativo",
    b.id as "id_beneficio"
    
from silver.insurance_registration ir
        left outer join silver.insurance_reg_set irs on irs.parent = ir.id
        left outer join silver.cliente clie on clie.codigo = ir.CUSTOMER_ID
        left outer join silver.catalogo cat on cat.cnpj_cpf = clie.cnpj_cpf
        left outer join silver.representante r on r.codigo = irs.id_unity
        left outer join silver.catalogo cata on cata.cnpj_cpf = r.cnpj_cpf
        left outer join silver.insurance_status iss on iss.id = irs.id_status
        left outer join silver.INSURANCE_REG_SET_COVERAGE irsc ON irsc.PARENT = irs.ID
        left outer join silver.INSURANCE_VEHICLE iv on iv.ID = irsc.ID_VEHICLE
        left outer join silver.TIPO_VEICULO tv on tv.CODIGO = iv.CODE_TYPE_VEHICLE 
        left outer join silver.INSURANCE_REG_SET_COV_TRAILER irsct on irsct.PARENT = irsc.ID
        left outer join silver.INSURANCE_TRAILER it on it.ID = irsct.ID_TRAILER
        left outer join silver.insurance_trailer itt on itt.id = irsc.ID_TRAILER
        left outer join silver.vendedor v on v.CODIGO = irs.ID_CONSULTANT
        left outer join silver.insurance_status isss on isss.id = irsc.id_status
        left outer join silver.price_list_benefits plb on plb.id = irsc.id_price_list
        left outer join silver.type_category tc on tc.id = plb.id_type_category
        left outer join silver.category c on c.id = tc.id_category
        left outer join silver.benefits b on b.id = c.id_benefits
        left outer join (
        select distinct 
            irs.id as conjunto,
            wb.user_name as suporte
    
            from silver.insurance_reg_set irs 
                inner join silver.web_user wb on wb.id = irs.id_user_support
            ) as user_name on user_name.conjunto = irs.id
            

where iss.id = 7
and (
    coalesce(iv.BOARD,it.BOARD,itt.BOARD) is not null
    and coalesce(iv.chassi,it.chassi,itt.chassi) is not null
)


and isss.description = 'ATIVO'

-------------------------------------------------------------------------------------------

union all

-------------------------------------------------------------------------------------------


SELECT DISTINCT
    
    coalesce(iv.BOARD,it.BOARD,itt.BOARD) as "placa",
    coalesce(iv.chassi,it.chassi,itt.chassi) as "chassi",
    coalesce(iv.id,it.id,itt.id) as "id_placa",
    coalesce(irsc.ID_VEHICLE) as "id_veiculo",
    coalesce(irsct.ID_TRAILER) as "id_carroceria",
    ir.id as "Matricula",
    irs.id as "Conjunto",
    cata.fantasia as "Unidade",
    v.DESCRICAO as "consultor",
    iss.description as "Status",
    cat.nome as "Cliente",
    CAST(irs.data_registration AS DATE) as "data_registro",
    cast(coalesce(irs.DATE_INITAL_EFFECT,date_add('day', -364, irs.DATE_FINAL_EFFECT), irs.date_activation) as date) as "data_ativacao",
    user_name.suporte as "Suporte",
    current_date AS "data_filtro",
    'Stcoop' as empresa,
    irsc.id as "coverage",
    b.description as "beneficio",
    c.DESCRIPTION as "categoria",
    tc.DESCRIPTION as "tipo_categoria",
    cast(coalesce(irsc.date_initial_effect,date_add('day', -364, irsc.date_final_effect)) as date) as "data_ativacao_beneficio",
    isss.description as "status_beneficio",
    CAST('1900-01-01 00:00:00' AS TIMESTAMP) 
    + (irsc.UPDATED_AT - 599266080000000000) / 864000000000 * INTERVAL '1' DAY AS "data_atualizacao",
    v.ativo as "consultor_ativo",
    b.id as "id_beneficio"
    
from stcoop.insurance_registration ir
        left outer join stcoop.insurance_reg_set irs on irs.parent = ir.id
        left outer join stcoop.cliente clie on clie.codigo = ir.CUSTOMER_ID
        left outer join stcoop.catalogo cat on cat.cnpj_cpf = clie.cnpj_cpf
        left outer join stcoop.representante r on r.codigo = irs.id_unity
        left outer join stcoop.catalogo cata on cata.cnpj_cpf = r.cnpj_cpf
        left outer join stcoop.insurance_status iss on iss.id = irs.id_status
        left outer join stcoop.INSURANCE_REG_SET_COVERAGE irsc ON irsc.PARENT = irs.ID
        left outer join stcoop.INSURANCE_VEHICLE iv on iv.ID = irsc.ID_VEHICLE
        left outer join stcoop.TIPO_VEICULO tv on tv.CODIGO = iv.CODE_TYPE_VEHICLE 
        left outer join stcoop.INSURANCE_REG_SET_COV_TRAILER irsct on irsct.PARENT = irsc.ID
        left outer join stcoop.INSURANCE_TRAILER it on it.ID = irsct.ID_TRAILER
        left outer join stcoop.insurance_trailer itt on itt.id = irsc.ID_TRAILER
        left outer join stcoop.vendedor v on v.CODIGO = irs.ID_CONSULTANT
        left outer join stcoop.insurance_status isss on isss.id = irsc.id_status
        left outer join stcoop.price_list_benefits plb on plb.id = irsc.id_price_list
        left outer join stcoop.type_category tc on tc.id = plb.id_type_category
        left outer join stcoop.category c on c.id = tc.id_category
        left outer join stcoop.benefits b on b.id = c.id_benefits
        left outer join (
        select distinct 
            irs.id as conjunto,
            wb.user_name as suporte
    
            from stcoop.insurance_reg_set irs 
                inner join stcoop.web_user wb on wb.id = irs.id_user_support
            ) as user_name on user_name.conjunto = irs.id
            

where iss.id = 7
and (
    coalesce(iv.BOARD,it.BOARD,itt.BOARD) is not null
    and coalesce(iv.chassi,it.chassi,itt.chassi) is not null
)


and isss.description = 'ATIVO'

-------------------------------------------------------------------------------------------

union all

-------------------------------------------------------------------------------------------


SELECT DISTINCT

    coalesce(iv.BOARD,it.BOARD,itt.BOARD) as "placa",
    coalesce(iv.chassi,it.chassi,itt.chassi) as "chassi",
    coalesce(iv.id,it.id,itt.id) as "id_placa",
    coalesce(irsc.ID_VEHICLE) as "id_veiculo",
    coalesce(irsct.ID_TRAILER) as "id_carroceria",
    ir.id as "Matricula",
    irs.id as "Conjunto",
    cata.fantasia as "Unidade",
    v.DESCRICAO as "consultor",
    iss.description as "Status",
    cat.nome as "Cliente",
    CAST(irs.data_registration AS DATE) as "data_registro",
    cast(coalesce(irs.DATE_INITAL_EFFECT,date_add('day', -364, irs.DATE_FINAL_EFFECT), irs.date_activation) as date) as "data_ativacao",
    user_name.suporte as "Suporte",
    current_date AS "data_filtro",
    'Viavante' as empresa,
    irsc.id as "coverage",
    b.DESCRIPTION as "beneficio",
    c.DESCRIPTION as "categoria",
    tc.DESCRIPTION as "tipo_categoria",
    cast(coalesce(irsc.date_initial_effect,date_add('day', -364, irsc.date_final_effect)) as date) as "data_ativacao_beneficio",
    isss.description as "status_beneficio",
    CAST('1900-01-01 00:00:00' AS TIMESTAMP) 
    + (irsc.UPDATED_AT - 599266080000000000) / 864000000000 * INTERVAL '1' DAY AS "data_atualizacao",
    v.ativo as "consultor_ativo",
    b.id as "id_beneficio"
    
from viavante.insurance_registration ir
        left outer join viavante.insurance_reg_set irs on irs.parent = ir.id
        left outer join viavante.cliente clie on clie.codigo = ir.CUSTOMER_ID
        left outer join viavante.catalogo cat on cat.cnpj_cpf = clie.cnpj_cpf
        left outer join viavante.representante r on r.codigo = irs.id_unity
        left outer join viavante.catalogo cata on cata.cnpj_cpf = r.cnpj_cpf
        left outer join viavante.insurance_status iss on iss.id = irs.id_status
        left outer join viavante.INSURANCE_REG_SET_COVERAGE irsc ON irsc.PARENT = irs.ID
        left outer join viavante.INSURANCE_VEHICLE iv on iv.ID = irsc.ID_VEHICLE
        left outer join viavante.TIPO_VEICULO tv on tv.CODIGO = iv.CODE_TYPE_VEHICLE 
        left outer join viavante.INSURANCE_REG_SET_COV_TRAILER irsct on irsct.PARENT = irsc.ID
        left outer join viavante.INSURANCE_TRAILER it on it.ID = irsct.ID_TRAILER
        left outer join viavante.insurance_trailer itt on itt.id = irsc.ID_TRAILER
        left outer join viavante.vendedor v on v.CODIGO = irs.ID_CONSULTANT
        left outer join viavante.insurance_status isss on isss.id = irsc.id_status
        left outer join viavante.price_list_benefits plb on plb.id = irsc.id_price_list
        left outer join viavante.type_category tc on tc.id = plb.id_type_category
        left outer join viavante.category c on c.id = tc.id_category
        left outer join viavante.benefits b on b.id = c.id_benefits
        left outer join (
        select distinct 
            irs.id as conjunto,
            wb.user_name as suporte
    
            from viavante.insurance_reg_set irs 
                inner join viavante.web_user wb on wb.id = irs.id_user_support
            ) as user_name on user_name.conjunto = irs.id
            

where iss.id = 7
and (
    coalesce(iv.BOARD,it.BOARD,itt.BOARD) is not null
    and coalesce(iv.chassi,it.chassi,itt.chassi) is not null
)


and isss.description = 'ATIVO'


-------------------------------------------------------------------------------------------

union all

-------------------------------------------------------------------------------------------


SELECT DISTINCT

    coalesce(iv.BOARD,it.BOARD,itt.BOARD) as "placa",
    coalesce(iv.chassi,it.chassi,itt.chassi) as "chassi",
    coalesce(iv.id,it.id,itt.id) as "id_placa",
    coalesce(irsc.ID_VEHICLE) as "id_veiculo",
    coalesce(irsct.ID_TRAILER) as "id_carroceria",
    ir.id as "Matricula",
    irs.id as "Conjunto",
    cata.fantasia as "Unidade",
    v.DESCRICAO as "consultor",
    iss.description as "Status",
    cat.nome as "Cliente",
    CAST(irs.data_registration AS DATE) as "data_registro",
    cast(coalesce(irs.DATE_INITAL_EFFECT,date_add('day', -364, irs.DATE_FINAL_EFFECT), irs.date_activation) as date) as "data_ativacao",
    user_name.suporte as "Suporte",
    current_date AS "data_filtro",
    'Tag' as empresa,
    irsc.id as "coverage",
    b.DESCRIPTION as "beneficio",
    c.DESCRIPTION as "categoria",
    tc.DESCRIPTION as "tipo_categoria",
    cast(coalesce(irsc.date_initial_effect,date_add('day', -364, irsc.date_final_effect)) as date) as "data_ativacao_beneficio",
    isss.description as "status_beneficio",
    CAST('1900-01-01 00:00:00' AS TIMESTAMP) 
    + (irsc.UPDATED_AT - 599266080000000000) / 864000000000 * INTERVAL '1' DAY AS "data_atualizacao",
    v.ativo as "consultor_ativo",
    b.id as "id_beneficio"
    
from tag.insurance_registration ir
        left outer join tag.insurance_reg_set irs on irs.parent = ir.id
        left outer join tag.cliente clie on clie.codigo = ir.CUSTOMER_ID
        left outer join tag.catalogo cat on cat.cnpj_cpf = clie.cnpj_cpf
        left outer join tag.representante r on r.codigo = irs.id_unity
        left outer join tag.catalogo cata on cata.cnpj_cpf = r.cnpj_cpf
        left outer join tag.insurance_status iss on iss.id = irs.id_status
        left outer join tag.INSURANCE_REG_SET_COVERAGE irsc ON irsc.PARENT = irs.ID
        left outer join tag.INSURANCE_VEHICLE iv on iv.ID = irsc.ID_VEHICLE
        left outer join tag.TIPO_VEICULO tv on tv.CODIGO = iv.CODE_TYPE_VEHICLE 
        left outer join tag.INSURANCE_REG_SET_COV_TRAILER irsct on irsct.PARENT = irsc.ID
        left outer join tag.INSURANCE_TRAILER it on it.ID = irsct.ID_TRAILER
        left outer join tag.insurance_trailer itt on itt.id = irsc.ID_TRAILER
        left outer join tag.vendedor v on v.CODIGO = irs.ID_CONSULTANT
        left outer join tag.insurance_status isss on isss.id = irsc.id_status
        left outer join tag.price_list_benefits plb on plb.id = irsc.id_price_list
        left outer join tag.type_category tc on tc.id = plb.id_type_category
        left outer join tag.category c on c.id = tc.id_category
        left outer join tag.benefits b on b.id = c.id_benefits
        left outer join (
        select distinct 
            irs.id as conjunto,
            wb.user_name as suporte
    
            from tag.insurance_reg_set irs 
                inner join tag.web_user wb on wb.id = irs.id_user_support
            ) as user_name on user_name.conjunto = irs.id
            

where iss.id = 7
and (
    coalesce(iv.BOARD,it.BOARD,itt.BOARD) is not null
    and coalesce(iv.chassi,it.chassi,itt.chassi) is not null
)


and isss.description = 'ATIVO'
and cast(coalesce(irsc.date_initial_effect,date_add('day', -364, irsc.date_final_effect)) as date) >= date('2025-08-01')