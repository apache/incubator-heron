#!/usr/bin/env python
import argparse
import string

RC_TEMPLATE = string.Template('''
apiVersion: v1
kind: ReplicationController
metadata:
  name: zookeeper
spec:
  replicas: 1
  template:
    metadata:
      name: zookeeper-${id}
      labels:
        app: zookeeper
        server-id: '${id}'
    spec:
      containers:
      - name: server
        image: fabric8/zookeeper
        env:
        - name: SERVER_ID
          value: '${id}'
        - name: MAX_SERVERS
          value: '${count}'
        ports:
        - containerPort: 2181
        - containerPort: 2888
        - containerPort: 3888
''')

SVC_TEMPLATE = string.Template('''
kind: Service
apiVersion: v1
metadata:
  name: zookeeper-${id}
spec:
  selector:
    app: zookeeper-${id}
  ports:
  - name: followers
    port: 2888
    targetPort: 2888
  - name: election
    port: 3888
    targetPort: 3888
  - name: client
    port: 2181
    targetPort: 2181
''')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Generate k8s zookeeper rc and svc based on https://github.com/fabric8io/fabric8-zookeeper-docker.')
    parser.add_argument('count', type=int, help='total number of zookeeper instance')

    args = parser.parse_args()

    for i in range(args.count):
        with open('rc/zookeeper-%d.yml' % i, 'w') as rc:
            rc.write(RC_TEMPLATE.substitute(count=args.count, id=i))
        with open('svc/zookeeper-%d.yml' % i, 'w') as svc:
            svc.write(SVC_TEMPLATE.substitute(count=args.count, id=i))
