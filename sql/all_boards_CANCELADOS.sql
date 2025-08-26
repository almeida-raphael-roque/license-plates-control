WITH CTE_segtruck AS (
    SELECT
        coalesce(iv.BOARD,it.BOARD,itt.BOARD) as "placa",
        coalesce(iv.chassi,it.chassi,itt.chassi) as "chassi",
        coalesce(iv.id,it.id,itt.id) as "id_placa",
        coalesce(irsc.ID_VEHICLE) as "id_veiculo",
        coalesce(irsct.ID_TRAILER) as "id_carroceria",
        ir.id as "Matricula",
        irs.id as "Conjunto",
        cata.fantasia as "Unidade",
        iss.description as "Status",
        cat.nome as "Cliente",
        irs.data_registration  as "Data",
        cast(irs.date_cancellation as date) as "data_cancelamento",
        irs.user_name_cancellation as "usuario_cancelamento",
        current_date AS "data_filtro",
        'Segtruck' as empresa,
        irh.data_registration as "data_registro_historico",
        -- irh.note as "Migração conferencia",
        CASE
            WHEN irh.note LIKE '%MIGRAÇÃO%' THEN 'SIM'
            ELSE 'NAO'
        END as "migracao",
        ROW_NUMBER() OVER (PARTITION BY coalesce(iv.BOARD,it.BOARD,itt.BOARD) ORDER BY irs.data_registration DESC) as rn,
        cast(irs.date_replaced as date) as "data_substituicao"
    FROM silver.insurance_registration ir
        LEFT JOIN silver.insurance_reg_set irs ON irs.parent = ir.id
        LEFT JOIN silver.cliente clie ON clie.codigo = ir.CUSTOMER_ID
        LEFT JOIN silver.catalogo cat ON cat.cnpj_cpf = clie.cnpj_cpf
        LEFT JOIN silver.representante r ON r.codigo = irs.id_unity
        LEFT JOIN silver.catalogo cata ON cata.cnpj_cpf = r.cnpj_cpf
        LEFT JOIN silver.insurance_status iss ON iss.id = irs.id_status
        LEFT JOIN silver.INSURANCE_REG_SET_COVERAGE irsc ON irsc.PARENT = irs.ID
        LEFT JOIN silver.INSURANCE_VEHICLE iv ON iv.ID = irsc.ID_VEHICLE
        LEFT JOIN silver.TIPO_VEICULO tv ON tv.CODIGO = iv.CODE_TYPE_VEHICLE 
        LEFT JOIN silver.INSURANCE_REG_SET_COV_TRAILER irsct ON irsct.PARENT = irsc.ID
        LEFT JOIN silver.INSURANCE_TRAILER it ON it.ID = irsct.ID_TRAILER
        LEFT JOIN silver.insurance_trailer itt ON itt.id = irsc.ID_TRAILER
        LEFT JOIN silver.insurance_reg_historic irh ON irh.id_set = irs.id
        LEFT JOIN (
            SELECT DISTINCT irs.id as conjunto, wb.user_name as suporte
            FROM silver.insurance_reg_set irs 
            INNER JOIN silver.web_user wb ON wb.id = irs.id_user_support
        ) as user_name ON user_name.conjunto = irs.id
    WHERE date_cancellation IS NOT NULL
        AND coalesce(iv.BOARD,it.BOARD,itt.BOARD) IS NOT NULL
        AND coalesce(iv.chassi,it.chassi,itt.chassi) IS NOT NULL
),

CTE_stcoop AS (
    SELECT
        coalesce(iv.BOARD,it.BOARD,itt.BOARD) as "placa",
        coalesce(iv.chassi,it.chassi,itt.chassi) as "chassi",
        coalesce(iv.id,it.id,itt.id) as "id_placa",
        coalesce(irsc.ID_VEHICLE) as "id_veiculo",
        coalesce(irsct.ID_TRAILER) as "id_carroceria",
        ir.id as "Matricula",
        irs.id as "Conjunto",
        CASE
            WHEN cata.nome LIKE '%FTR%' THEN cata.nome
            ELSE cata.FANTASIA
        END as "Unidade",
        iss.description as "Status",
        cat.nome as "Cliente",
        irs.data_registration  as "Data",
        cast(irs.date_cancellation as date) as "data_cancelamento",
        irs.user_name_cancellation as "usuario_cancelamento",
        current_date AS "data_filtro",
        'Stcoop' as empresa,
        irh.data_registration as "data_registro_historico",
        -- irh.note as "Migração conferencia",
        CASE
            WHEN irh.note LIKE '%MIGRAÇÃO%' THEN 'SIM'
            ELSE 'NAO'
        END as "migracao",
        ROW_NUMBER() OVER (PARTITION BY coalesce(iv.BOARD,it.BOARD,itt.BOARD) ORDER BY irs.data_registration DESC) as rn,
        cast(irs.date_replaced as date) as "data_substituicao"
    FROM stcoop.insurance_registration ir
        LEFT JOIN stcoop.insurance_reg_set irs ON irs.parent = ir.id
        LEFT JOIN stcoop.cliente clie ON clie.codigo = ir.CUSTOMER_ID
        LEFT JOIN stcoop.catalogo cat ON cat.cnpj_cpf = clie.cnpj_cpf
        LEFT JOIN stcoop.representante r ON r.codigo = irs.id_unity
        LEFT JOIN stcoop.catalogo cata ON cata.cnpj_cpf = r.cnpj_cpf
        LEFT JOIN stcoop.insurance_status iss ON iss.id = irs.id_status
        LEFT JOIN stcoop.INSURANCE_REG_SET_COVERAGE irsc ON irsc.PARENT = irs.ID
        LEFT JOIN stcoop.INSURANCE_VEHICLE iv ON iv.ID = irsc.ID_VEHICLE
        LEFT JOIN stcoop.TIPO_VEICULO tv ON tv.CODIGO = iv.CODE_TYPE_VEHICLE 
        LEFT JOIN stcoop.INSURANCE_REG_SET_COV_TRAILER irsct ON irsct.PARENT = irsc.ID
        LEFT JOIN stcoop.INSURANCE_TRAILER it ON it.ID = irsct.ID_TRAILER
        LEFT JOIN stcoop.insurance_trailer itt ON itt.id = irsc.ID_TRAILER
        LEFT JOIN stcoop.insurance_reg_historic irh ON irh.id_set = irs.id
        LEFT JOIN (
            SELECT DISTINCT irs.id as conjunto, wb.user_name as suporte
            FROM stcoop.insurance_reg_set irs 
            INNER JOIN stcoop.web_user wb ON wb.id = irs.id_user_support
        ) as user_name ON user_name.conjunto = irs.id
    WHERE date_cancellation IS NOT NULL
        AND coalesce(iv.BOARD,it.BOARD,itt.BOARD) IS NOT NULL
        AND coalesce(iv.chassi,it.chassi,itt.chassi) IS NOT NULL
),

CTE_viavante AS (
    SELECT
        coalesce(iv.BOARD,it.BOARD,itt.BOARD) as "placa",
        coalesce(iv.chassi,it.chassi,itt.chassi) as "chassi",
        coalesce(iv.id,it.id,itt.id) as "id_placa",
        coalesce(irsc.ID_VEHICLE) as "id_veiculo",
        coalesce(irsct.ID_TRAILER) as "id_carroceria",
        ir.id as "Matricula",
        irs.id as "Conjunto",
        CASE
            WHEN cata.nome LIKE '%FTR%' THEN cata.nome
            ELSE cata.FANTASIA
        END as "Unidade",
        iss.description as "Status",
        cat.nome as "Cliente",
        irs.data_registration  as "Data",
        cast(irs.date_cancellation as date) as "data_cancelamento",
        irs.user_name_cancellation as "usuario_cancelamento",
        current_date AS "data_filtro",
        'Viavante' as empresa,
        irh.data_registration as "data_registro_historico",
        -- irh.note as "Migração conferencia",
        CASE
            WHEN irh.note LIKE '%MIGRAÇÃO%' THEN 'SIM'
            ELSE 'NAO'
        END as "migracao",
        ROW_NUMBER() OVER (PARTITION BY coalesce(iv.BOARD,it.BOARD,itt.BOARD) ORDER BY irs.data_registration DESC) as rn,
        cast(irs.date_replaced as date) as "data_substituicao"
    FROM viavante.insurance_registration ir
        -- joins como estavam
        LEFT JOIN viavante.insurance_reg_set irs ON irs.parent = ir.id
        LEFT JOIN viavante.cliente clie ON clie.codigo = ir.CUSTOMER_ID
        LEFT JOIN viavante.catalogo cat ON cat.cnpj_cpf = clie.cnpj_cpf
        LEFT JOIN viavante.representante r ON r.codigo = irs.id_unity
        LEFT JOIN viavante.catalogo cata ON cata.cnpj_cpf = r.cnpj_cpf
        LEFT JOIN viavante.insurance_status iss ON iss.id = irs.id_status
        LEFT JOIN viavante.INSURANCE_REG_SET_COVERAGE irsc ON irsc.PARENT = irs.ID
        LEFT JOIN viavante.INSURANCE_VEHICLE iv ON iv.ID = irsc.ID_VEHICLE
        LEFT JOIN viavante.TIPO_VEICULO tv ON tv.CODIGO = iv.CODE_TYPE_VEHICLE 
        LEFT JOIN viavante.INSURANCE_REG_SET_COV_TRAILER irsct ON irsct.PARENT = irsc.ID
        LEFT JOIN viavante.INSURANCE_TRAILER it ON it.ID = irsct.ID_TRAILER
        LEFT JOIN viavante.insurance_trailer itt ON itt.id = irsc.ID_TRAILER
        LEFT JOIN viavante.insurance_reg_historic irh ON irh.id_set = irs.id
        LEFT JOIN (
            SELECT DISTINCT irs.id as conjunto, wb.user_name as suporte
            FROM viavante.insurance_reg_set irs 
            INNER JOIN viavante.web_user wb ON wb.id = irs.id_user_support
        ) as user_name ON user_name.conjunto = irs.id
    WHERE date_cancellation IS NOT NULL
        AND coalesce(iv.BOARD,it.BOARD,itt.BOARD) IS NOT NULL
        AND coalesce(iv.chassi,it.chassi,itt.chassi) IS NOT NULL
),

CTE_tag AS (
    SELECT
        coalesce(iv.BOARD,it.BOARD,itt.BOARD) as "placa",
        coalesce(iv.chassi,it.chassi,itt.chassi) as "chassi",
        coalesce(iv.id,it.id,itt.id) as "id_placa",
        coalesce(irsc.ID_VEHICLE) as "id_veiculo",
        coalesce(irsct.ID_TRAILER) as "id_carroceria",
        ir.id as "Matricula",
        irs.id as "Conjunto",
        CASE
            WHEN cata.nome LIKE '%FTR%' THEN cata.nome
            ELSE cata.FANTASIA
        END as "Unidade",
        iss.description as "Status",
        cat.nome as "Cliente",
        irs.data_registration  as "Data",
        cast(irs.date_cancellation as date) as "data_cancelamento",
        irs.user_name_cancellation as "usuario_cancelamento",
        current_date AS "data_filtro",
        'Viavante' as empresa,
        irh.data_registration as "data_registro_historico",
        -- irh.note as "Migração conferencia",
        CASE
            WHEN irh.note LIKE '%MIGRAÇÃO%' THEN 'SIM'
            ELSE 'NAO'
        END as "migracao",
        ROW_NUMBER() OVER (PARTITION BY coalesce(iv.BOARD,it.BOARD,itt.BOARD) ORDER BY irs.data_registration DESC) as rn,
        cast(irs.date_replaced as date) as "data_substituicao"
    FROM tag.insurance_registration ir
        -- joins como estavam
        LEFT JOIN tag.insurance_reg_set irs ON irs.parent = ir.id
        LEFT JOIN tag.cliente clie ON clie.codigo = ir.CUSTOMER_ID
        LEFT JOIN tag.catalogo cat ON cat.cnpj_cpf = clie.cnpj_cpf
        LEFT JOIN tag.representante r ON r.codigo = irs.id_unity
        LEFT JOIN tag.catalogo cata ON cata.cnpj_cpf = r.cnpj_cpf
        LEFT JOIN tag.insurance_status iss ON iss.id = irs.id_status
        LEFT JOIN tag.INSURANCE_REG_SET_COVERAGE irsc ON irsc.PARENT = irs.ID
        LEFT JOIN tag.INSURANCE_VEHICLE iv ON iv.ID = irsc.ID_VEHICLE
        LEFT JOIN tag.TIPO_VEICULO tv ON tv.CODIGO = iv.CODE_TYPE_VEHICLE 
        LEFT JOIN tag.INSURANCE_REG_SET_COV_TRAILER irsct ON irsct.PARENT = irsc.ID
        LEFT JOIN tag.INSURANCE_TRAILER it ON it.ID = irsct.ID_TRAILER
        LEFT JOIN tag.insurance_trailer itt ON itt.id = irsc.ID_TRAILER
        LEFT JOIN tag.insurance_reg_historic irh ON irh.id_set = irs.id
        LEFT JOIN (
            SELECT DISTINCT irs.id as conjunto, wb.user_name as suporte
            FROM tag.insurance_reg_set irs 
            INNER JOIN tag.web_user wb ON wb.id = irs.id_user_support
        ) as user_name ON user_name.conjunto = irs.id
    WHERE date_cancellation IS NOT NULL
        AND coalesce(iv.BOARD,it.BOARD,itt.BOARD) IS NOT NULL
        AND coalesce(iv.chassi,it.chassi,itt.chassi) IS NOT NULL
        AND cast(irs.date_cancellation as date) >= date('2025-08-01')
        AND cast(irs.date_replaced as date) >= DATE('2025-08-01')
)

SELECT 
    "placa",
    "chassi",
    "id_placa",
    "id_veiculo",
    "id_carroceria",
    "Matricula",
    "Conjunto",
    "Unidade",
    "Status",
    "Cliente",
    "Data",
    "data_cancelamento",
    "usuario_cancelamento",
    "data_filtro",
    "empresa",
    "data_registro_historico",
    "migracao",
    "data_substituicao"
FROM (
    SELECT * FROM CTE_segtruck WHERE rn = 1
    UNION ALL
    SELECT * FROM CTE_stcoop WHERE rn = 1
    UNION ALL
    SELECT * FROM CTE_viavante WHERE rn = 1
    UNION ALL
    SELECT * FROM CTE_tag WHERE rn = 1
) AS final
