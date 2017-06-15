
-type(member_status() :: joining | up | leaving | suspect | down).

-type(member_address() :: {inet:ip_address(), inet:port_number()}).

-record(member, {node, guid, status}).

-type(member() :: #member{}).

