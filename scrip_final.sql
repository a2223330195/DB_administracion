-- =====================================================================
-- SQL Unificado: Nueva Estructura + Correcciones Entregable 5
-- Descripción: Este script combina la estructura de DWH optimizada con
-- las correcciones de seguridad, rendimiento y monitoreo.

DROP DATABASE IF EXISTS productos_bd;
CREATE DATABASE productos_bd
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;
USE productos_bd;

SET FOREIGN_KEY_CHECKS=0;
DROP TABLE IF EXISTS
    Transacciones, Tickets, Productos,
    fact_ventas, dim_tiempo, dim_producto, dim_cliente, -- Tablas DWH
    market_basket_analysis, categoria_proximidad, patron_recomendaciones, anomalias_compra, bundling_opportunities,
    frecuentes_apriori, reglas_apriori, comparativo_mba_apriori,
    dw_log_ejecucion, dw_progreso_runtime, dw_tiempos_pasos, dw_auditoria_proceso;

-- Se separan los DROP en sentencias individuales para máxima compatibilidad.
DROP VIEW IF EXISTS SoportesIndividuales;
DROP VIEW IF EXISTS Pares;
DROP VIEW IF EXISTS CategoriasUnicasPorTicket;
DROP VIEW IF EXISTS ParesDeCategorias;
DROP VIEW IF EXISTS BundlesCalculados;

DROP PROCEDURE IF EXISTS registrar_tiempo_paso;
DROP PROCEDURE IF EXISTS sp_procesar_casos_de_uso_v2;
DROP PROCEDURE IF EXISTS commit_progreso;
DROP PROCEDURE IF EXISTS sp_procesar_casos_de_uso_simple;

SET FOREIGN_KEY_CHECKS=1;

-- Dimensiones del Data Warehouse
CREATE TABLE dim_tiempo (
    fecha_key INT UNSIGNED PRIMARY KEY,
    fecha DATE NOT NULL,
    anio SMALLINT UNSIGNED NOT NULL,
    mes TINYINT UNSIGNED NOT NULL,
    dia TINYINT UNSIGNED NOT NULL,
    dia_semana TINYINT UNSIGNED NOT NULL, -- 1=Domingo, 7=Sábado
    nombre_mes VARCHAR(15) NOT NULL, -- CORRECCIÓN: Aumentado para nombres largos como 'Septiembre'
    nombre_dia_semana VARCHAR(15) NOT NULL, -- CORRECCIÓN: Aumentado para nombres largos como 'Miércoles'
    trimestre TINYINT UNSIGNED NOT NULL,
    semana_anio TINYINT UNSIGNED NOT NULL,
    es_fin_semana TINYINT(1) NOT NULL,
    periodo_anio CHAR(7) NOT NULL, -- Formato 'YYYY-MM'
    temporada VARCHAR(15) NOT NULL,
    INDEX idx_fecha (fecha)
) ENGINE=InnoDB;

CREATE TABLE dim_producto (
    producto_key INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    producto_id INT UNSIGNED NOT NULL UNIQUE,
    nombre_producto VARCHAR(255) NOT NULL,
    categoria VARCHAR(100) NOT NULL,
    precio_actual DECIMAL(10,2) NOT NULL,
    INDEX idx_prod_cat (categoria)
) ENGINE=InnoDB;

CREATE TABLE dim_cliente (
    cliente_key INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    -- Tabla preparada para futura segmentación de clientes (ej. por RFM, demografía). Actualmente solo se usa un cliente 'General'.
    tipo_cliente VARCHAR(20) NOT NULL,
    UNIQUE KEY uq_tipo_cliente (tipo_cliente)
) ENGINE=InnoDB;

-- Tabla de Hechos del Data Warehouse
CREATE TABLE fact_ventas (
    venta_id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    fecha_key INT UNSIGNED NOT NULL,
    producto_key INT UNSIGNED NOT NULL,
    cliente_key INT UNSIGNED NOT NULL,
    ticket_id INT UNSIGNED NOT NULL,
    cantidad_vendida INT UNSIGNED NOT NULL,
    precio_total DECIMAL(10,2) NOT NULL,
    monto_total_ticket DECIMAL(12,2) NOT NULL,
    cantidad_productos_en_ticket INT UNSIGNED NOT NULL,
    UNIQUE KEY uq_ticket_producto (ticket_id, producto_key),
    INDEX idx_ticket (ticket_id),
    INDEX idx_fecha (fecha_key),
    INDEX idx_producto (producto_key),
    FOREIGN KEY (fecha_key) REFERENCES dim_tiempo(fecha_key),
    FOREIGN KEY (producto_key) REFERENCES dim_producto(producto_key),
    FOREIGN KEY (cliente_key) REFERENCES dim_cliente(cliente_key)
) ENGINE=InnoDB;

-- Tablas Analíticas y de Resultados
CREATE TABLE market_basket_analysis (
    producto_a_key INT UNSIGNED NOT NULL,
    producto_b_key INT UNSIGNED NOT NULL,
    producto_a_hash CHAR(32) NOT NULL,
    producto_b_hash CHAR(32) NOT NULL,
    soporte_conjunto INT UNSIGNED NOT NULL,
    soporte_a INT UNSIGNED NOT NULL, 
    soporte_b INT UNSIGNED NOT NULL,
    confianza_a_b DECIMAL(10,6) NOT NULL,
    confianza_b_a DECIMAL(10,6) NOT NULL,
    lift DECIMAL(10,6) NOT NULL,
    jaccard_index DECIMAL(10,6),
    categoria_a VARCHAR(100) NOT NULL,
    categoria_b VARCHAR(100) NOT NULL,
    fecha_calculo DATE NOT NULL,
    UNIQUE KEY uq_mba_pair (producto_a_key, producto_b_key),
    INDEX idx_mba_lift (lift),
    -- MEJORA: Índices para acelerar búsquedas de la API por producto individual.
    INDEX idx_mba_prod_a (producto_a_key),
    INDEX idx_mba_prod_b (producto_b_key)
) ENGINE=InnoDB;

CREATE TABLE categoria_proximidad (
    categoria_a VARCHAR(100) NOT NULL,
    categoria_b VARCHAR(100) NOT NULL,
    frecuencia_conjunta INT UNSIGNED NOT NULL,
    total_transacciones INT UNSIGNED NOT NULL,
    score_proximidad DECIMAL(10,6) NOT NULL,
    nivel_prioridad VARCHAR(10) NOT NULL,
    temporada_peak VARCHAR(15),
    fecha_analisis DATE NOT NULL,    
    PRIMARY KEY (categoria_a, categoria_b)
) ENGINE=InnoDB;

CREATE TABLE patron_recomendaciones (
    producto_trigger_key INT UNSIGNED NOT NULL,
    producto_recomendado_key INT UNSIGNED NOT NULL,
    tipo_recomendacion VARCHAR(30) NOT NULL,
    score_recomendacion DECIMAL(10,6) NOT NULL,
    probabilidad_compra DECIMAL(10,6) NOT NULL,
    incremento_ticket_promedio DECIMAL(10,2) NOT NULL,
    mensaje_recomendacion TEXT NOT NULL,
    fecha_creacion DATE NOT NULL,
    fuente VARCHAR(15) NOT NULL,
    PRIMARY KEY (producto_trigger_key, producto_recomendado_key, fuente),
    INDEX idx_rec_trigger (producto_trigger_key),
    INDEX idx_rec_recomendado (producto_recomendado_key),
    INDEX idx_rec_fuente (fuente)
) ENGINE=InnoDB;

CREATE TABLE anomalias_compra (
    id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    ticket_id INT UNSIGNED NOT NULL,
    tipo_anomalia VARCHAR(30) NOT NULL,
    score_anomalia DECIMAL(10,6) NOT NULL,
    descripcion_anomalia TEXT NOT NULL,
    -- MEJORA: Se añade un ID autoincremental como PK. Se mantiene un índice único para la lógica de negocio.
    UNIQUE KEY uq_ticket_anomalia (ticket_id, tipo_anomalia),
    INDEX idx_anom_score (score_anomalia DESC)
) ENGINE=InnoDB;

CREATE TABLE bundling_opportunities (
    nombre_bundle VARCHAR(255) NOT NULL,
    productos_bundle TEXT NOT NULL,
    PRIMARY KEY (nombre_bundle),
    tipo_bundle VARCHAR(30) NOT NULL,
    -- Aumentada la precisión para coincidir con el cálculo intermedio en el SP y evitar overflow.
    potencial_incremento DECIMAL(22,4) NOT NULL,
    INDEX idx_bundle_pot (potencial_incremento)
) ENGINE=InnoDB;
-- TABLAS APRIORI
CREATE TABLE IF NOT EXISTS frecuentes_apriori (
    itemset_hash CHAR(32) PRIMARY KEY,
    itemset_text VARCHAR(500) NOT NULL,
    tam_itemset TINYINT UNSIGNED NOT NULL,
    soporte_abs INT UNSIGNED NOT NULL,
    soporte_rel DECIMAL(10,6) NOT NULL,
    fecha_calculo DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_itemset_text (itemset_text(255)) -- Índice único opcional en el texto para búsquedas rápidas
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS reglas_apriori (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    antecedente_text VARCHAR(400) NOT NULL,
    consecuente_text VARCHAR(400) NOT NULL,
    antecedente_hash CHAR(32) NOT NULL,
    consecuente_hash CHAR(32) NOT NULL,
    soporte_abs INT UNSIGNED NOT NULL,
    soporte_rel DECIMAL(10,6) NOT NULL,
    confianza DECIMAL(10,6) NOT NULL,
    lift DECIMAL(10,6) NOT NULL,
    conviccion DECIMAL(12,6) NOT NULL DEFAULT 999999.00, -- CORRECCIÓN: Default a un número alto para representar "infinito"
    leverage DECIMAL(10,6),
    fecha_calculo DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    fuente VARCHAR(20) NOT NULL DEFAULT 'APRIORI',
    UNIQUE KEY uq_regla_hash (antecedente_hash, consecuente_hash),
    INDEX idx_regla_lift (lift),
    INDEX idx_regla_conf (confianza),
    -- MEJORA: Índices para acelerar búsquedas de la API por antecedente o consecuente.
    INDEX idx_regla_antecedente (antecedente_hash),
    INDEX idx_regla_consecuente (consecuente_hash)
) ENGINE=InnoDB;

CREATE TABLE comparativo_mba_apriori (
    producto_a_key INT UNSIGNED NOT NULL,
    producto_b_key INT UNSIGNED NOT NULL,
    conf_mba DECIMAL(10,6),
    conf_apriori DECIMAL(10,6),
    lift_mba DECIMAL(10,6),
    lift_apriori DECIMAL(10,6),
    delta_conf DECIMAL(10,6),
    delta_lift DECIMAL(10,6),
    fuente_prioritaria VARCHAR(15),
    fecha_calculo DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (producto_a_key, producto_b_key)
) ENGINE=InnoDB;

-- Tablas de Control y Monitoreo
CREATE TABLE IF NOT EXISTS dw_log_ejecucion(
  id INT AUTO_INCREMENT PRIMARY KEY,
  timestamp DATETIME NOT NULL,
  mensaje TEXT NOT NULL,
  tipo_log VARCHAR(20) NOT NULL,
  INDEX idx_tipo_time (tipo_log, timestamp)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS dw_progreso_runtime (
    id INT AUTO_INCREMENT PRIMARY KEY,
    paso_actual VARCHAR(100) NOT NULL, -- El id=1 se usará por defecto para el proceso principal
    indice INT NOT NULL DEFAULT 0,
    total INT NOT NULL DEFAULT 1,
    pct DECIMAL(5,2) NOT NULL DEFAULT 0.00,
    inicio DATETIME NOT NULL,
    ultima_actualizacion DATETIME NOT NULL
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS dw_tiempos_pasos (
    id INT AUTO_INCREMENT PRIMARY KEY,
    paso VARCHAR(100) NOT NULL,
    segundos DECIMAL(10,2) NOT NULL,
    fecha_ejecucion DATETIME NOT NULL,
    INDEX idx_paso_fecha (paso, fecha_ejecucion)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS dw_auditoria_proceso (
    id INT AUTO_INCREMENT PRIMARY KEY,
    inicio_proceso DATETIME NOT NULL,
    fin_proceso DATETIME,
    duracion_segundos DECIMAL(10,2),
    total_transacciones_procesadas INT UNSIGNED,
    status_final VARCHAR(20),
    mensaje_final TEXT,
    comentario VARCHAR(255)
) ENGINE=InnoDB;

-- Procedimientos Almacenados
DELIMITER $$

-- Procedimiento auxiliar para confirmar el progreso de forma aislada
CREATE PROCEDURE commit_progreso(IN p_paso VARCHAR(100), IN p_indice INT, IN p_total INT)
BEGIN
    -- Este procedimiento se ejecuta en su propia transacción para que el progreso sea visible
    START TRANSACTION;
    -- por el script de monitoreo, incluso si el SP principal está en una transacción larga.
    -- Se utiliza INSERT ... ON DUPLICATE KEY UPDATE para una operación "upsert" atómica y más limpia.
    INSERT INTO dw_progreso_runtime(id, paso_actual, indice, total, pct, inicio, ultima_actualizacion)
    VALUES (1, p_paso, p_indice, p_total, COALESCE((p_indice / NULLIF(p_total, 0)) * 100, 0.00), NOW(), NOW())
    ON DUPLICATE KEY UPDATE
        paso_actual = VALUES(paso_actual),
        indice = VALUES(indice),
        pct = VALUES(pct),
        ultima_actualizacion = VALUES(ultima_actualizacion);
    COMMIT; -- Confirma solo esta actualización de progreso
END$$

CREATE PROCEDURE registrar_tiempo_paso(
    IN p_paso VARCHAR(100),
    INOUT p_t_paso_inicio DATETIME(6)
)
BEGIN
    DECLARE v_segundos DECIMAL(10,2);
    -- Si p_t_paso_inicio es NULL (primera llamada), inicializarlo para evitar error en TIMEDIFF.
    SET p_t_paso_inicio = COALESCE(p_t_paso_inicio, NOW(6));
    SET v_segundos = TIME_TO_SEC(TIMEDIFF(NOW(6), p_t_paso_inicio));
    INSERT INTO dw_tiempos_pasos(paso, segundos, fecha_ejecucion)
    VALUES(p_paso, v_segundos, NOW());
    SET p_t_paso_inicio = NOW(6);
END$$

CREATE PROCEDURE sp_procesar_casos_de_uso_v2(
    IN p_min_sop_rel DECIMAL(10,8),
    IN p_ejecutar_analyze TINYINT
)
main_proc: BEGIN
    DECLARE total_tickets INT;
    DECLARE t0 DATETIME(6) DEFAULT NOW(6);
    DECLARE t_paso_inicio DATETIME(6);
    DECLARE pasos INT DEFAULT 12; -- Aumentamos el número de pasos a 12
    DECLARE v_min_pair_abs INT DEFAULT 10;
    DECLARE v_pairs_limit INT DEFAULT 2000000; -- Límite de seguridad para pares (ajustable)
    DECLARE v_count_mba INT DEFAULT 0;
    DECLARE v_count_cat INT DEFAULT 0;
    DECLARE v_count_rec INT DEFAULT 0;
    DECLARE v_count_anom INT DEFAULT 0;
    DECLARE v_count_bnd INT DEFAULT 0;
    DECLARE v_count_cmp INT DEFAULT 0;
    DECLARE v_apriori_tiene_datos INT DEFAULT 0;
    DECLARE v_fact_rows INT DEFAULT 0;
    -- Variables para procesamiento por batches en Market Basket
    DECLARE v_min_prod_a INT DEFAULT 0;
    DECLARE v_max_prod_a INT DEFAULT 0;
    DECLARE v_batch_prod_cnt INT DEFAULT 1000; -- número de valores distintos de prod_a por batch (ajustable)
    DECLARE v_inserted_total BIGINT DEFAULT 0;
    DECLARE v_rows_batch INT DEFAULT 0;

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        GET DIAGNOSTICS CONDITION 1
            @p1 = RETURNED_SQLSTATE, @p2 = MESSAGE_TEXT;

        INSERT INTO dw_log_ejecucion(timestamp, mensaje, tipo_log)
        VALUES(NOW(), CONCAT('ERROR SQL: ', @p2, ' (SQLSTATE: ', @p1, ')'), 'ERROR');

        UPDATE dw_progreso_runtime
        SET paso_actual = 'ERROR', ultima_actualizacion = NOW()
        WHERE id = 1;

        SET SESSION unique_checks = 1;
        SET SESSION foreign_key_checks = 1;

        ROLLBACK;
        RESIGNAL;
    END;

    -- Inicialización y optimización de sesión
    SET t_paso_inicio = NOW(6);
    SET SESSION unique_checks = 0;
    SET SESSION foreign_key_checks = 0;
    REPLACE INTO dw_progreso_runtime(id, paso_actual, indice, total, pct, inicio, ultima_actualizacion) -- Esta primera actualización se confirma con el CALL a commit_progreso
    VALUES(1, 'Inicio', 0, pasos, 0.0, NOW(), NOW());

    SELECT COUNT(*) INTO v_fact_rows FROM Transacciones;
    IF v_fact_rows = 0 THEN
        INSERT INTO dw_log_ejecucion(timestamp, mensaje, tipo_log) VALUES(NOW(), 'Proceso abortado: La tabla Transacciones está vacía.', 'WARN');
        LEAVE main_proc;
    END IF;

    INSERT INTO dw_log_ejecucion(timestamp, mensaje, tipo_log) VALUES(NOW(), 'Iniciando SP v2 (Optimizado)', 'INFO');
    
    -- Evitar usar information_schema.table_rows (no fiable para InnoDB). Usar un EXISTS determinista.
    SELECT EXISTS(SELECT 1 FROM reglas_apriori LIMIT 1) INTO v_apriori_tiene_datos;
    
    CALL registrar_tiempo_paso('Inicialización', t_paso_inicio);

    -- ESTA ES LA VERSIÓN CORREGIDA (RÁPIDA)
    CALL commit_progreso('Limpieza DW', 1, pasos);
    TRUNCATE TABLE fact_ventas;
    TRUNCATE TABLE market_basket_analysis;
    TRUNCATE TABLE categoria_proximidad;
    TRUNCATE TABLE patron_recomendaciones;
    TRUNCATE TABLE anomalias_compra;
    TRUNCATE TABLE bundling_opportunities;
    TRUNCATE TABLE comparativo_mba_apriori;
    -- Las tablas de Apriori se limpian en el script de Python, pero se pueden añadir aquí si se desea centralizar.
    -- DELETE FROM reglas_apriori; DELETE FROM frecuentes_apriori;
    DROP VIEW IF EXISTS SoportesIndividuales, Pares, CategoriasUnicasPorTicket, ParesDeCategorias, BundlesCalculados, C3_Candidatos;

    CALL registrar_tiempo_paso('Limpieza DW', t_paso_inicio);

    -- Carga de Dimensiones (lógica incremental)
    CALL commit_progreso('Dimensiones', 2, pasos);
    -- La carga de dim_tiempo se ha movido al script de Python (entregable_final.py)
    -- para asegurar que los datos existen ANTES de que el proceso Apriori (Entregable 2) los necesite.
    -- Esto resuelve un problema de dependencia donde el muestreo estratificado fallaba en la primera ejecución.
    -- El SP ahora solo se encarga de las otras dimensiones.

    INSERT INTO dim_producto (producto_id, nombre_producto, categoria, precio_actual) SELECT Identificador, Producto, categoria, Precio FROM Productos ON DUPLICATE KEY UPDATE nombre_producto=VALUES(nombre_producto), categoria=VALUES(categoria), precio_actual=VALUES(precio_actual);
    INSERT IGNORE INTO dim_cliente (cliente_key, tipo_cliente) VALUES (1, 'General');
    CALL registrar_tiempo_paso('Dimensiones', t_paso_inicio);

    -- Carga de Tabla de Hechos
    CALL commit_progreso('Fact Ventas', 3, pasos);
    -- Carga de fact_ventas en lotes para evitar transacciones largas y mejorar el monitoreo.
    SELECT COUNT(*) INTO v_fact_rows FROM Transacciones;
    IF v_fact_rows = 0 THEN
        INSERT INTO dw_log_ejecucion(timestamp, mensaje, tipo_log) VALUES(NOW(), 'Carga de Hechos omitida - sin datos en Transacciones', 'INFO');
    ELSE
    fact_load_loop: BEGIN
        DECLARE v_last_trans_id INT UNSIGNED DEFAULT 0;
        DECLARE v_batch_size INT DEFAULT 100000; -- Lotes de 100,000 filas
        DECLARE v_rows_in_batch INT DEFAULT 0;
        DECLARE v_total_processed INT DEFAULT 0;
        DECLARE v_max_id_in_batch INT UNSIGNED;
        DECLARE v_max_trans_id INT UNSIGNED;

        -- 1. Obtener el ID máximo de Transacciones antes de entrar al bucle.
        SELECT MAX(id) INTO v_max_trans_id FROM Transacciones;
        -- 2. Validar que v_max_trans_id no sea NULL antes de entrar al bucle.
        SET v_max_trans_id = COALESCE(v_max_trans_id, 0);

        -- Se optimiza el bucle para evitar una doble lectura de la tabla Transacciones.
        -- En lugar de hacer un INSERT...LIMIT y luego un SELECT MAX(id) sobre el mismo rango,        
        -- primero se determina el ID máximo del lote y luego se inserta usando ese rango.
        -- Esto reduce a la mitad las operaciones de lectura en cada iteración.
        WHILE v_last_trans_id < v_max_trans_id DO
            -- 1. Determinar el ID máximo del siguiente lote.
            SELECT MAX(id) INTO v_max_id_in_batch FROM (
                SELECT id FROM Transacciones WHERE id > v_last_trans_id ORDER BY id LIMIT v_batch_size
            ) AS subquery_max_id;

            -- CORRECCIÓN CRÍTICA: Si la subconsulta no devuelve más filas, v_max_id_in_batch será NULL.
            -- Se debe detectar este caso para salir del bucle y evitar un ciclo infinito.
            IF v_max_id_in_batch IS NULL THEN
                LEAVE fact_load_loop;
            END IF;

            -- 2. Insertar todas las filas hasta ese ID máximo.
            INSERT INTO fact_ventas (fecha_key, producto_key, cliente_key, ticket_id, cantidad_vendida, precio_total, monto_total_ticket, cantidad_productos_en_ticket)
            SELECT
                CAST(DATE_FORMAT(t.fecha, '%Y%m%d') AS UNSIGNED),
                dp.producto_key, 1, tr.id_ticket, tr.cantidad_productos, tr.precio_venta,
                t.monto_total, t.cantidad_productos_vendidos
            FROM Transacciones tr
            INNER JOIN Tickets t ON tr.id_ticket = t.id
            INNER JOIN dim_producto dp ON tr.producto_id = dp.producto_id
            WHERE tr.id > v_last_trans_id AND tr.id <= v_max_id_in_batch;

            SET v_total_processed = v_total_processed + ROW_COUNT();
            SET v_last_trans_id = v_max_id_in_batch; -- 3. Actualizar el puntero para la siguiente iteración.
            CALL commit_progreso(CONCAT('Fact Ventas (', v_total_processed, ' de ', v_fact_rows, ')'), v_total_processed, v_fact_rows);
        END WHILE;        
    END fact_load_loop;
    END IF;
    SELECT COUNT(*) INTO total_tickets FROM Tickets;
    SET total_tickets = COALESCE(total_tickets, 0); -- Asegurar que no sea NULL
    CALL registrar_tiempo_paso('Fact Ventas', t_paso_inicio);

    -- Si no hay datos en la tabla de hechos, no tiene sentido ejecutar el resto de los análisis.
    SELECT COUNT(*) INTO v_fact_rows FROM fact_ventas;
    IF v_fact_rows = 0 THEN
        INSERT INTO dw_log_ejecucion(timestamp, mensaje, tipo_log) VALUES(NOW(), 'Análisis omitido - fact_ventas está vacía.', 'WARN');
        -- Finalización temprana
        LEAVE main_proc;
    END IF;

    -- Market Basket Analysis (consulta optimizada y segura)
    CALL commit_progreso('Market Basket', 4, pasos);
    -- Asegurar que el soporte mínimo absoluto sea al menos 1 para evitar errores con 0.
    SET v_min_pair_abs = GREATEST(CEIL(total_tickets * p_min_sop_rel), 1);

    -- OPTIMIZACIÓN CRÍTICA PARA GRANDES VOLÚMENES:
    -- Crear tablas de staging con sufijo único por conexión para evitar colisiones
    -- y el error 1137. El bloque Market Basket se ejecuta como SQL dinámico
    -- usando estos nombres y se limpian al finalizar.
    SET @suffix = CONNECTION_ID();
    SET @tbl = CONCAT('staging_productos_por_ticket_', @suffix);
    SET @tbl_read = CONCAT('staging_productos_por_ticket_read_', @suffix);

    -- Crear tabla staging dinámica
    SET @sql = CONCAT('DROP TABLE IF EXISTS ', @tbl);
    PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

    SET @sql = CONCAT('CREATE TABLE ', @tbl, ' (ticket_id INT UNSIGNED, producto_key INT UNSIGNED, PRIMARY KEY (ticket_id, producto_key)) ENGINE=InnoDB');
    PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

    SET @sql = CONCAT('INSERT INTO ', @tbl, ' (ticket_id, producto_key) SELECT DISTINCT ticket_id, producto_key FROM fact_ventas');
    PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

    -- Crear copia de solo lectura (separar DROP y CREATE)
    SET @sql = CONCAT('DROP TABLE IF EXISTS ', @tbl_read);
    PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

    SET @sql = CONCAT('CREATE TABLE ', @tbl_read, ' ENGINE=InnoDB AS SELECT ticket_id, producto_key FROM ', @tbl);
    PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

    -- Refactor Market Basket: materializar pares en tabla staging y usar sentencias separadas
    -- Crear tabla de soportes individuales materializada (staging) por conexión
    SET @tbl_support = CONCAT('staging_soportes_', @suffix);
    SET @sql = CONCAT('DROP TABLE IF EXISTS ', @tbl_support);
    PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

    SET @sql = CONCAT('CREATE TABLE ', @tbl_support, ' ENGINE=InnoDB AS SELECT producto_key, COUNT(DISTINCT ticket_id) AS soporte_individual FROM fact_ventas GROUP BY producto_key');
    PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

    -- Crear tabla de pares staging (prod_a, prod_b, conjunto_count)
    SET @tbl_pairs = CONCAT('staging_pairs_', @suffix);
    SET @sql = CONCAT('DROP TABLE IF EXISTS ', @tbl_pairs);
    PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

    SET @sql = CONCAT('CREATE TABLE ', @tbl_pairs, ' (prod_a INT UNSIGNED, prod_b INT UNSIGNED, conjunto_count BIGINT, PRIMARY KEY(prod_a, prod_b)) ENGINE=InnoDB');
    PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

    -- Poblar la tabla de pares agrupando sobre la copia de solo-lectura
    SET @sql = CONCAT(
        'INSERT INTO ', @tbl_pairs, ' (prod_a, prod_b, conjunto_count) ',
        'SELECT f1.producto_key AS prod_a, f2.producto_key AS prod_b, COUNT(*) AS conjunto_count ',
        'FROM ', @tbl_read, ' f1 JOIN ', @tbl_read, ' f2 ON f1.ticket_id = f2.ticket_id AND f1.producto_key < f2.producto_key ',
        'GROUP BY prod_a, prod_b HAVING conjunto_count >= ', CAST(v_min_pair_abs AS CHAR)
    );
    PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

    -- Validar tamaño de la tabla de pares antes de continuar (limite de seguridad)
    SET @sql = CONCAT('SELECT COUNT(*) FROM ', @tbl_pairs);
    PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
    SELECT COUNT(*) INTO v_count_mba FROM (SELECT 1) AS dummy; -- placeholder para asegurar variable existe
    -- Reemplazar el recuento real (ejecutamos SELECT COUNT(*) directamente)
    SET @cnt_sql = CONCAT('SELECT COUNT(*) FROM ', @tbl_pairs);
    PREPARE stmt_cnt FROM @cnt_sql; EXECUTE stmt_cnt; DEALLOCATE PREPARE stmt_cnt;
    -- Obtener el resultado del último SELECT a partir de una variable temporal
    -- (MySQL no devuelve automáticamente a variables desde EXECUTE; usaremos una tabla temporal rápida)
    DROP TABLE IF EXISTS __tmp_mba_count;
    CREATE TABLE __tmp_mba_count (cnt BIGINT);
    SET @sql = CONCAT('INSERT INTO __tmp_mba_count SELECT COUNT(*) FROM ', @tbl_pairs);
    PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
    SELECT cnt INTO v_count_mba FROM __tmp_mba_count LIMIT 1;
    DROP TABLE IF EXISTS __tmp_mba_count;

    -- Realizar un commit intermedio para liberar locks y no mantener transacción larga
    COMMIT;

    IF v_count_mba = 0 THEN
        INSERT INTO dw_log_ejecucion(timestamp, mensaje, tipo_log) VALUES(NOW(), CONCAT('MBA omitted: no pairs generated (suffix=', @suffix, ')'), 'INFO');
        -- Limpiar staging dinámico
        SET @sql = CONCAT('DROP TABLE IF EXISTS ', @tbl); PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
        SET @sql = CONCAT('DROP TABLE IF EXISTS ', @tbl_read); PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
        SET @sql = CONCAT('DROP TABLE IF EXISTS ', @tbl_pairs); PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
        SET @sql = CONCAT('DROP TABLE IF EXISTS ', @tbl_support); PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
        CALL registrar_tiempo_paso('Market Basket', t_paso_inicio);
    ELSEIF v_count_mba > v_pairs_limit THEN
        INSERT INTO dw_log_ejecucion(timestamp, mensaje, tipo_log) VALUES(NOW(), CONCAT('MBA aborted: pairs count = ', v_count_mba, ' exceeds safety limit (', v_pairs_limit, '). Suffix=', @suffix), 'WARN');
        -- Limpiar staging dinámico
        SET @sql = CONCAT('DROP TABLE IF EXISTS ', @tbl); PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
        SET @sql = CONCAT('DROP TABLE IF EXISTS ', @tbl_read); PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
        SET @sql = CONCAT('DROP TABLE IF EXISTS ', @tbl_pairs); PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
        SET @sql = CONCAT('DROP TABLE IF EXISTS ', @tbl_support); PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
        CALL registrar_tiempo_paso('Market Basket', t_paso_inicio);
    ELSE
        INSERT INTO dw_log_ejecucion(timestamp, mensaje, tipo_log) VALUES(NOW(), CONCAT('MBA: pairs generated = ', v_count_mba, ', proceeding to populate market_basket_analysis (suffix=', @suffix, ')'), 'INFO');

    -- Ejecutar INSERT final por batches para evitar operaciones masivas que saturen la BD.
    -- Estrategia: iterar sobre valores de prod_a (ordenados) en bloques y procesar cada bloque por separado.
    -- Esto permite commits intermedios, menor locking y mejor control de recursos.

    -- Determinar rango de prod_a
        SET @sql = CONCAT('SELECT MIN(prod_a), MAX(prod_a) FROM ', @tbl_pairs);
        PREPARE stmt_range FROM @sql; EXECUTE stmt_range; DEALLOCATE PREPARE stmt_range;
        -- Recuperar resultados a variables temporales mediante tabla auxiliar
        DROP TABLE IF EXISTS __tmp_range; CREATE TABLE __tmp_range (min_a INT, max_a INT);
        SET @sql = CONCAT('INSERT INTO __tmp_range SELECT MIN(prod_a), MAX(prod_a) FROM ', @tbl_pairs);
        PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
        SELECT min_a, max_a INTO v_min_prod_a, v_max_prod_a FROM __tmp_range LIMIT 1;
        DROP TABLE IF EXISTS __tmp_range;

        -- Si no hay rango válido, saltar
        IF v_min_prod_a IS NULL OR v_max_prod_a IS NULL THEN
            SET v_count_mba = 0;
        ELSE
            SET v_count_mba = 0;
            batch_loop: WHILE v_min_prod_a <= v_max_prod_a DO
                -- Seleccionar un batch de prod_a
                DROP TABLE IF EXISTS __tmp_batch_prod_a;
                CREATE TEMPORARY TABLE __tmp_batch_prod_a (prod_a INT PRIMARY KEY) ENGINE=MEMORY;
                SET @sql = CONCAT('INSERT INTO __tmp_batch_prod_a SELECT DISTINCT prod_a FROM ', @tbl_pairs, ' WHERE prod_a >= ', v_min_prod_a, ' ORDER BY prod_a LIMIT ', v_batch_prod_cnt);
                PREPARE stmt_batch FROM @sql; EXECUTE stmt_batch; DEALLOCATE PREPARE stmt_batch;

                -- Si la tabla temporal de batch está vacía, salir
                SELECT COUNT(*) INTO v_rows_batch FROM __tmp_batch_prod_a;
                IF v_rows_batch = 0 THEN
                    DROP TEMPORARY TABLE IF EXISTS __tmp_batch_prod_a;
                    LEAVE batch_loop;
                END IF;

                -- Insertar los pares correspondientes a los prod_a del batch y calcular métricas
                SET @sql = CONCAT(
                    'INSERT INTO market_basket_analysis (producto_a_key, producto_b_key, producto_a_hash, producto_b_hash, soporte_conjunto, soporte_a, soporte_b, confianza_a_b, confianza_b_a, lift, jaccard_index, categoria_a, categoria_b, fecha_calculo) ',
                    'SELECT p.prod_a, p.prod_b, MD5(CONCAT(CHAR(91), p.prod_a, CHAR(93))), MD5(CONCAT(CHAR(91), p.prod_b, CHAR(93))), p.conjunto_count, sa.soporte_individual, sb.soporte_individual, ',
                    '(p.conjunto_count / NULLIF(sa.soporte_individual,0)), (p.conjunto_count / NULLIF(sb.soporte_individual,0)), ',
                    '(p.conjunto_count * ', CAST(total_tickets AS CHAR), ') / NULLIF(sa.soporte_individual * sb.soporte_individual, 0), ',
                    'p.conjunto_count / NULLIF(sa.soporte_individual + sb.soporte_individual - p.conjunto_count,0), pa_dim.categoria, pb_dim.categoria, CURDATE() ',
                    'FROM ', @tbl_pairs, ' p JOIN __tmp_batch_prod_a b ON p.prod_a = b.prod_a ',
                    'JOIN ', @tbl_support, ' sa ON p.prod_a = sa.producto_key JOIN ', @tbl_support, ' sb ON p.prod_b = sb.producto_key ',
                    'JOIN dim_producto pa_dim ON p.prod_a = pa_dim.producto_key JOIN dim_producto pb_dim ON p.prod_b = pb_dim.producto_key'
                );
                PREPARE stmt_insert_batch FROM @sql; EXECUTE stmt_insert_batch; DEALLOCATE PREPARE stmt_insert_batch;

                -- Contar filas insertadas en este batch
                SELECT ROW_COUNT() INTO v_rows_batch;
                SET v_inserted_total = v_inserted_total + v_rows_batch;
                SET v_count_mba = v_count_mba + v_rows_batch;

                -- Commit intermedio y registro
                COMMIT;
                INSERT INTO dw_log_ejecucion(timestamp, mensaje, tipo_log) VALUES(NOW(), CONCAT('MBA batch inserted rows=', v_rows_batch, ', cumulative=', v_inserted_total, ', suffix=', @suffix), 'INFO');

                -- Preparar el siguiente batch (el mayor prod_a procesado + 1)
                SELECT MAX(prod_a) INTO v_min_prod_a FROM __tmp_batch_prod_a;
                SET v_min_prod_a = COALESCE(v_min_prod_a, 0) + 1;
                DROP TEMPORARY TABLE IF EXISTS __tmp_batch_prod_a;
            END WHILE batch_loop;
        END IF;

        -- Antes de limpiar, registrar el total real insertado en market_basket_analysis
    SET v_count_mba = v_inserted_total;
    INSERT INTO dw_log_ejecucion(timestamp, mensaje, tipo_log) VALUES(NOW(), CONCAT('MBA final inserted_total=', v_inserted_total, ', suffix=', @suffix), 'INFO');

    -- Limpiar tablas staging dinámicas (DROP por separado)
    SET @sql = CONCAT('DROP TABLE IF EXISTS ', @tbl); PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
    SET @sql = CONCAT('DROP TABLE IF EXISTS ', @tbl_read); PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
    SET @sql = CONCAT('DROP TABLE IF EXISTS ', @tbl_pairs); PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
    SET @sql = CONCAT('DROP TABLE IF EXISTS ', @tbl_support); PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

        CALL registrar_tiempo_paso('Market Basket', t_paso_inicio);
    END IF;

    -- Proximidad de Categorías
    CALL commit_progreso('Categoria Proximidad', 5, pasos);
    INSERT INTO categoria_proximidad (categoria_a, categoria_b, frecuencia_conjunta, total_transacciones, score_proximidad, nivel_prioridad, temporada_peak, fecha_analisis)
    -- OPTIMIZACIÓN CRÍTICA: Evitar el auto-join masivo en Transacciones.
    -- 1. Se agrupan las categorías por ticket en una tabla temporal.
    -- 2. Se hace el auto-join sobre esta tabla temporal, que es mucho más pequeña.
    WITH CategoriasUnicasPorTicket AS (
        -- La consulta debe basarse en las tablas del DWH (fact_ventas y dimensiones), no en las tablas operacionales.
        -- Esto asegura que el análisis se realiza sobre los datos limpios y ya procesados.
        SELECT DISTINCT fv.ticket_id, dp.categoria
        FROM fact_ventas fv
        JOIN dim_producto dp ON fv.producto_key = dp.producto_key
    ),
    -- 2. Ahora el auto-join es sobre un conjunto de datos mucho más pequeño y manejable.
    ParesDeCategorias AS (
        SELECT c1.categoria AS cat_a, c2.categoria AS cat_b, COUNT(*) AS frec_conjunta
        FROM CategoriasUnicasPorTicket c1
        JOIN (SELECT ticket_id, categoria FROM CategoriasUnicasPorTicket) c2 ON c1.ticket_id = c2.ticket_id AND c1.categoria < c2.categoria
        GROUP BY cat_a, cat_b
        HAVING frec_conjunta >= 5 -- Filtrar pares con muy baja frecuencia
    )
    SELECT cat_a, cat_b, frec_conjunta, total_tickets, (frec_conjunta / NULLIF(total_tickets, 0)),
           CASE WHEN frec_conjunta >= total_tickets * 0.01 THEN 'Alta' WHEN frec_conjunta >= total_tickets * 0.005 THEN 'Media' ELSE 'Baja' END,
           'General', CURDATE()
    FROM ParesDeCategorias;
    SELECT ROW_COUNT() INTO v_count_cat;
    CALL registrar_tiempo_paso('Categoria Proximidad', t_paso_inicio);

    -- Patrones de Recomendaciones
    CALL commit_progreso('Recomendaciones', 6, pasos);
    INSERT INTO patron_recomendaciones (producto_trigger_key, producto_recomendado_key, tipo_recomendacion, score_recomendacion, probabilidad_compra, incremento_ticket_promedio, mensaje_recomendacion, fecha_creacion, fuente)
    SELECT mba.producto_a_key, mba.producto_b_key, 'Market Basket', ((mba.confianza_a_b * mba.lift) / 2), mba.confianza_a_b, (dp2.precio_actual * mba.confianza_a_b), CONCAT('Clientes que compran ', dp1.nombre_producto, ' también compran ', dp2.nombre_producto), CURDATE(), 'MBA'
    FROM market_basket_analysis mba
    JOIN dim_producto dp1 ON dp1.producto_key = mba.producto_a_key
    JOIN dim_producto dp2 ON dp2.producto_key = mba.producto_b_key
    WHERE mba.confianza_a_b >= 0.20 AND mba.lift >= 1.5; -- SINCRONIZADO: Umbral de confianza alineado con el MIN_CONFIANZA de Python (0.20)
    SELECT ROW_COUNT() INTO v_count_rec;
    CALL registrar_tiempo_paso('Recomendaciones', t_paso_inicio);

    -- Detección de Anomalías (basado en desviación estándar)
    CALL commit_progreso('Anomalias', 7, pasos);
    INSERT INTO anomalias_compra (ticket_id, tipo_anomalia, score_anomalia, descripcion_anomalia)
    SELECT t.id, 'Valor_Extremo', ((t.monto_total - stats.promedio) / NULLIF(stats.desv_std, 0)), CONCAT('Ticket con valor ', ROUND(t.monto_total, 2), ' vs promedio ', ROUND(stats.promedio, 2))
    FROM Tickets t CROSS JOIN (SELECT AVG(monto_total) as promedio, STDDEV(monto_total) as desv_std FROM Tickets) stats
    WHERE ABS(t.monto_total - stats.promedio) > 2.5 * stats.desv_std;
    SELECT ROW_COUNT() INTO v_count_anom;
    CALL registrar_tiempo_paso('Anomalias', t_paso_inicio);

    -- Oportunidades de Bundling
    CALL commit_progreso('Bundling', 8, pasos);
    INSERT INTO bundling_opportunities (nombre_bundle, tipo_bundle, potencial_incremento, productos_bundle)
    WITH BundlesCalculados AS (
        SELECT 
            dp1.nombre_producto AS nombre_a,
            dp2.nombre_producto AS nombre_b,
            -- Se usa CAST con mayor precisión para asegurar que el cálculo intermedio no cause overflow.
            CAST(mba.lift * (dp1.precio_actual + dp2.precio_actual) * mba.confianza_a_b AS DECIMAL(22,4)) AS potencial
        FROM market_basket_analysis mba
        JOIN dim_producto dp1 ON dp1.producto_key = mba.producto_a_key
        JOIN dim_producto dp2 ON dp2.producto_key = mba.producto_b_key
        WHERE mba.lift >= 1.5 AND mba.confianza_a_b >= 0.20 -- SINCRONIZADO: Umbral de confianza alineado con el MIN_CONFIANZA de Python (0.20)
    )
    SELECT CONCAT('Bundle_', REPLACE(nombre_a, ' ', '_'), '_', REPLACE(nombre_b, ' ', '_')), 'Producto_Cross', LEAST(potencial, 9999999999.9999), CONCAT(nombre_a, ' + ', nombre_b)
    FROM BundlesCalculados ORDER BY potencial DESC LIMIT 100;
    SELECT ROW_COUNT() INTO v_count_bnd;
    CALL registrar_tiempo_paso('Bundling', t_paso_inicio);

    -- Comparativo MBA vs Apriori
    CALL commit_progreso('Comparativo Apriori', 9, pasos);
    IF v_apriori_tiene_datos > 0 THEN
        -- Se usa una tabla temporal para evitar ambigüedades en el LEFT JOIN y mejorar el rendimiento.
        -- Se buscan las reglas de Apriori en ambas direcciones (A->B y B->A) y se elige la de mayor confianza.
        DROP TEMPORARY TABLE IF EXISTS temp_reglas_apriori_pares;
        CREATE TEMPORARY TABLE temp_reglas_apriori_pares (
            `hash1` char(32) NOT NULL, `hash2` char(32) NOT NULL, `confianza` decimal(10,6) DEFAULT NULL, `lift` decimal(10,6) DEFAULT NULL, PRIMARY KEY (`hash1`,`hash2`)
        );

        -- Insertar reglas A->B y B->A para poder hacer un solo JOIN
    INSERT INTO temp_reglas_apriori_pares (hash1, hash2, confianza, lift)
    SELECT antecedente_hash, consecuente_hash, confianza, lift FROM reglas_apriori
    WHERE fuente = 'APRIORI' AND antecedente_text LIKE '[%]' AND consecuente_text LIKE '[%]'
    ON DUPLICATE KEY UPDATE confianza = GREATEST(temp_reglas_apriori_pares.confianza, VALUES(confianza));
        
        INSERT INTO comparativo_mba_apriori (producto_a_key, producto_b_key, conf_mba, conf_apriori, lift_mba, lift_apriori, delta_conf, delta_lift, fuente_prioritaria, fecha_calculo)
        SELECT
            mba.producto_a_key,
            mba.producto_b_key,
            GREATEST(mba.confianza_a_b, mba.confianza_b_a),
            COALESCE(MAX(apr.confianza), 0),
            mba.lift,
            COALESCE(MAX(apr.lift), 0),
            GREATEST(mba.confianza_a_b, mba.confianza_b_a) - COALESCE(MAX(apr.confianza), 0),
            mba.lift - COALESCE(MAX(apr.lift), 0),
            CASE WHEN mba.lift >= COALESCE(MAX(apr.lift), 0) THEN 'MBA' ELSE 'APRIORI' END,
            NOW()
        FROM market_basket_analysis mba
        LEFT JOIN temp_reglas_apriori_pares apr ON 
            (apr.hash1 = mba.producto_a_hash AND apr.hash2 = mba.producto_b_hash) OR
            (apr.hash1 = mba.producto_b_hash AND apr.hash2 = mba.producto_a_hash)
        GROUP BY mba.producto_a_key, mba.producto_b_key, mba.confianza_a_b, mba.confianza_b_a, mba.lift;
        
        SELECT ROW_COUNT() INTO v_count_cmp;

        DROP TEMPORARY TABLE IF EXISTS temp_reglas_apriori_pares;
    ELSE
        SET v_count_cmp = 0;
        INSERT INTO dw_log_ejecucion(timestamp, mensaje, tipo_log) VALUES(NOW(), 'Comparativo omitido - sin datos Apriori', 'INFO');
    END IF;
    CALL registrar_tiempo_paso('Comparativo', t_paso_inicio);

    -- Análisis Opcional de Tablas
    CALL commit_progreso('Analizando tablas', 10, pasos);
    IF COALESCE(p_ejecutar_analyze, 0) = 1 THEN
        ANALYZE TABLE fact_ventas, market_basket_analysis, categoria_proximidad, patron_recomendaciones;
    END IF;
    CALL registrar_tiempo_paso('Analyze', t_paso_inicio);

    -- Finalización y Auditoría
    CALL commit_progreso('Finalizando', 11, pasos);
    SET SESSION unique_checks = 1;
    SET SESSION foreign_key_checks = 1;
    INSERT INTO dw_auditoria_proceso(inicio_proceso, fin_proceso, duracion_segundos, total_transacciones_procesadas, status_final, mensaje_final, comentario)
    VALUES(t0, NOW(), TIME_TO_SEC(TIMEDIFF(NOW(), t0)), v_fact_rows, 'COMPLETADO', CONCAT('MBA:', v_count_mba, ', Cat:', v_count_cat, ', Rec:', v_count_rec, ', Anom:', v_count_anom, ', Bnd:', v_count_bnd, ', Cmp:', v_count_cmp), CONCAT('Soporte Relativo: ', p_min_sop_rel));
    CALL registrar_tiempo_paso('Finalización', t_paso_inicio);

    -- Commit final (paso visible para monitoreo)
    CALL commit_progreso('Realizando Commit...', 12, pasos);
    -- El commit se realiza automáticamente al final del hilo en Python si no hay errores.
    -- Esta actualización solo sirve para notificar al usuario.

END main_proc$$

DELIMITER ;

DELIMITER $$

DROP PROCEDURE IF EXISTS sp_procesar_casos_de_uso_simple$$
CREATE PROCEDURE sp_procesar_casos_de_uso_simple()
BEGIN
    -- Wrapper para compatibilidad con ejecuciones simples
    CALL sp_procesar_casos_de_uso_v2(0.01, 1);
END$$
DELIMITER ;
