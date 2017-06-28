{application, ekka, [
	{description, "Autocluster and Autoheal for EMQ"},
	{vsn, "0.1"},
	{modules, ['ekka','ekka_app','ekka_boot','ekka_cluster','ekka_gossip','ekka_guid','ekka_membership','ekka_mnesia','ekka_node','ekka_node_discover','ekka_node_monitor','ekka_node_sup','ekka_sup']},
	{registered, [ekka_sup]},
	{applications, [kernel,stdlib,mnesia,lager]},
	{mod, {ekka_app, []}}
]}.