
Relies on the KVP table to store the state of the streaming application.
The state is concurently updated in an optimistic way relying on the `version` field of the KVP table and retrying in case of conflict.


![Drag Racing](/images/use-cases/stateful-streaming.png)