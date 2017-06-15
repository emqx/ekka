{application, ekka, [
	{description, "Distributed Layer for EMQ"},
	{vsn, "0.0.1"},
	{modules, ['ekka','ekka_app','ekka_autoheal','ekka_boot','ekka_cluster','ekka_gossip','ekka_guid','ekka_mnesia','ekka_node','ekka_node_discover','ekka_node_monitor','ekka_node_sup','ekka_rpc','ekka_sup']},
	{registered, [ekka_sup]},
	{applications, [kernel,stdlib,mnesia]},
	{mod, {ekka_app, []}}
]}.