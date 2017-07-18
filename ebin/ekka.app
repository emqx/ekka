{application, ekka, [
	{description, "Autocluster and Autoheal for EMQ"},
	{vsn, "0.1"},
	{modules, ['ekka','ekka_app','ekka_autocluster','ekka_autodown','ekka_autoheal','ekka_boot','ekka_cluster','ekka_cluster_consul','ekka_cluster_dns','ekka_cluster_etcd','ekka_cluster_k8s','ekka_cluster_mcast','ekka_cluster_static','ekka_cluster_strategy','ekka_cluster_sup','ekka_guid','ekka_httpc','ekka_membership','ekka_mnesia','ekka_node','ekka_node_monitor','ekka_node_sup','ekka_sup']},
	{registered, [ekka_sup]},
	{applications, [kernel,stdlib,mnesia,lager]},
	{mod, {ekka_app, []}}
]}.