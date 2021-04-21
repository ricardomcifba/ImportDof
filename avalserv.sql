select *,
    --Ganhos sobre aplicação do Hidrômetro
    fat_aguaesgoto - ref_fat_sah as ganho_fat_ah,
    arrec_aguaesgoto - ref_arrec_sah as ganho_arrec_ah,
    
    --Avaliação Financeira
    ganho_fat_aguaesgoto * 0.0925 as piscofins_ganho_fat,
    ganho_fat_aguaesgoto_m4 * 0.0925 as piscofins_ganho_fat_m4,
    (fat_aguaesgoto - ref_fat_sah) * 0.0925 as piscofins_ganho_fat_ah
    
 from (

 
select vi.*,
    --Informações de Recomendação
    (select descricao as r
     from amf_envio e 
     join amf_prioridade p on e.prioridade = p.sigla 
     where e.matricula = v.matricula and v.tipo_servico_executado = e.tiposervico 
     order by marcada desc limit 1) as recomendado,
     
    str_motivo_exec_servicos(v.matricula, servicos_executados, data_execucao) as motivo,
     
     --Informações de Execução do Serviço
    (select id||' - '||descricao from amf_tiposervico s 
    where s.id = v.tipo_servico_executado) as tipo_servico_executado,
    subtipo_servico,
    data_execucao,
    to_char(data_execucao, 'MM/YYYY') as mes_exec,
   (select concat(t.id || '-' || t.descricao, ', ') from amf_tiposervico t where t.id = any(servicos_executados)) as todos_servicos_exec,
    str_hist_exec_servicos(historico_execucao) as hist_exec_servicos,
    case when total_serv_tipo1 > 0 then 'S' end as houve_serv_tipo1,
    case when total_serv_tipo2 > 0 then 'S' end as houve_serv_tipo2,
    case when total_serv_tipo3 > 0 then 'S' end as houve_serv_tipo3,
    case when total_serv_tipo4 > 0 then 'S' end as houve_serv_tipo4,
    case when total_serv_tipo5 > 0 then 'S' end as houve_serv_tipo5,
    case when total_serv_tipo6 > 0 then 'S' end as houve_serv_tipo6,
    case when total_serv_tipo7 > 0 then 'S' end as houve_serv_tipo7,
    case when total_serv_tipo8 > 0 then 'S' end as houve_serv_tipo8,
    case when total_serv_tipo9 > 0 then 'S' end as houve_serv_tipo9,
    total_serv_tipo1,
    total_serv_tipo2,
    total_serv_tipo3,
    total_serv_tipo4,
    total_serv_tipo5,
    total_serv_tipo6,
    total_serv_tipo7,
    total_serv_tipo8,
    total_serv_tipo9, 
    hd_instalado,
    hd_anterior,
    idade_instalacao_hidrometro(v.matricula, hd_anterior, data_execucao) as idade_hd_anterior,
    idade_fabricacao_hidrometro(hd_anterior, data_execucao) as idade_fabr_hd_anterior,
    str_vazao_hidrometro(hd_anterior) as vazao_hd_anterior,
    str_marca_hidrometro(hd_anterior) as marca_hd_anterior,
    idade_fabricacao_hidrometro(hd_anterior, data_execucao) as idade_fabr_hd_instalado,
    str_vazao_hidrometro(hd_instalado) as vazao_hd_instalado,
    str_marca_hidrometro(hd_instalado) as marca_hd_instalado,
    tag,
    (select concat(classe, ',') 
     from amf_usuariocmpc 
     where matricula = vi.matricula) as classe_mpc,

     --Contas Faturadas
    ultimo_mes_fat,
    ref_seq(seq_ref_consumo(v.matricula, v.data_execucao)) as mes_cronofat,

    --Contagem de Meses
    meses_fat_aposexec,
    meses_deb_aposexec,
    meses_arrec,
    meses_arrec_cons,
    meses_deb_m4,
    meses_arrec_pot,

    --Previsões sem Execução
    vol_efetivo - ganho_vol_efetivo as prevse_vol_efetivo,
    vol_faturado - ganho_vol_faturado as prevse_vol_faturado,
    fat_aguaesgoto - ganho_fat_aguaesgoto as prevse_fat_aguaesgoto,
    fat_aguaesgoto_m4 - ganho_fat_aguaesgoto_m4 as prevse_fat_aguaesgoto_m4,

    --prevse_base_arrec_aguaesgoto,
    prevse_arrec_execserv(perfil_arrecadacao, prevse_base_arrec_aguaesgoto) as prevse_arrec_aguaesgoto,
    prevse_arrec_execserv(perfil_arrecadacao, prevse_base_arrec_vnae_aposexec) as prevse_arrec_vnae_aguaesgoto,
    prevse_arrec_execserv(perfil_arrecadacao, prevse_base_arrec_aguaesgoto) +
    prevse_arrec_execserv(perfil_arrecadacao, prevse_base_arrec_vnae_aposexec) as prevse_arrec,
    arrec_pot_aguaesgoto - ganho_arrec_pot_aguaesgoto as prevse_arrec_pot_aguaesgoto,


    -- Volumes e Faturamento Realizados
    vol_efetivo,
    vol_faturado,
    fat_aguaesgoto,
    fat_efetivo_aguaesgoto,
    fat_servicos,
    fat_aguaesgoto_m4,
    fat_efetivo_aguaesgoto_m4,

    --Ganhos
    ganho_vol_efetivo,
    ganho_vol_faturado,
    ganho_fat_aguaesgoto,
    ganho_fat_aguaesgoto_m4,
    arrec_aguaesgoto - prevse_arrec_execserv(perfil_arrecadacao, prevse_base_arrec_aguaesgoto) as ganho_arrec_aguaesgoto,
    arrec_vnae_aposexec - prevse_arrec_execserv(perfil_arrecadacao, prevse_base_arrec_vnae_aposexec) as ganho_arrec_vnae_aposexec,

    arrec_vnae_aposexec - prevse_arrec_execserv(perfil_arrecadacao, prevse_base_arrec_vnae_aposexec) +
    arrec_aguaesgoto - prevse_arrec_execserv(perfil_arrecadacao, prevse_base_arrec_aguaesgoto) as ganho_arrec,

    ganho_arrec_aguaesgoto_pagas,
    ganho_arrec_pot_aguaesgoto,

    --Posições sobre Ganho
    posicao_ganho(ganho_vol_faturado) as pos_vol_faturado,
    posicao_ganho(ganho_fat_aguaesgoto) as pos_fat_aguaesgoto,
    posicao_ganho(arrec_aguaesgoto - prevse_arrec_execserv(perfil_arrecadacao, prevse_base_arrec_aguaesgoto)) as pos_arrec_aguaesgoto,

    --Débitos / Contas em aberto
    fat_efetivo_aguaesgoto - arrec_nom_aguaesgoto as debito_aguaesgoto_aposexec,
    fat_efetivo_aguaesgoto_m4 - arrec_nom_aguaesgoto_m4 as debito_aguaesgoto_m4,
    fat_efetivo_servicos_m4 - arrec_nom_servicos_m4 as debito_servicos_m4,
    fat_efetivo_servicos - arrec_nom_servicos as debito_servicos_aposexec,

    --Débitos de Evasão
    case when perfil_arrecadacao in ('Devedor', 'Recuperado', 'Em Aberto') then 0.00
         else debito_aguaesgoto_m4 end as debito_evasao_aguaesgoto_m4,
    case when perfil_arrecadacao in ('Devedor', 'Recuperado', 'Em Aberto') then 0.00
         else debito_aguaesgoto_m4 + debito_servicos_m4 end as debito_evasao_m4,
         
    --Simulações de Faturamento Mínimo / Sem Aplicação de Hidrômetro
    simul_volfat_trfminima,
    simul_fat_trfminima,
    simul_arrec_trfminima_pagas,
    case when (total_serv_tipo4 > 0 or total_serv_tipo6 > 0) then simul_volfat_trfminima 
         when (total_serv_tipo1 > 0 or total_serv_tipo2 > 0) then vol_faturado - ganho_vol_faturado 
         else vol_faturado end as ref_vol_faturado_sah,
    case when (total_serv_tipo4 > 0 or total_serv_tipo6 > 0) then simul_fat_trfminima 
         when (total_serv_tipo1 > 0 or total_serv_tipo2 > 0) then fat_aguaesgoto - ganho_fat_aguaesgoto
         else fat_aguaesgoto end as ref_fat_sah,
    case when (total_serv_tipo4 > 0 or total_serv_tipo6 > 0) then simul_arrec_trfminima_pagas
         when (total_serv_tipo1 > 0 or total_serv_tipo2 > 0) then prevse_arrec_execserv(perfil_arrecadacao, prevse_base_arrec_aguaesgoto)
         else arrec_aguaesgoto end as ref_arrec_sah,

    --Arrecadação
    perfil_arrecadacao as perfil_arrecadacao,
    arrec_aguaesgoto,
    arrec_nom_aguaesgoto_m4 as arrec_aguaesgoto_m4,
    arrec_servicos,
    arrec_vnae_aposexec,
    arrec_vnae_m4,
    arrec_nom_aguaesgoto_m4 + arrec_nom_servicos_m4 as arrec_m4,

    --Arrecadação Em Potencial (Contas em Aberto após M4)
    arrec_pot_aguaesgoto,
    arrec_pot_servicos


from view_infousuario_exec_servicos vi 
join amf_resumo_ganhos(array[@servicos], '@localidade', @ano, null, null) v on vi.matricula = v.matricula and vi.localidade = v.localidade
where ganho_vol_faturado is not null
  and ganho_fat_aguaesgoto is not null) z;
