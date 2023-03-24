;; Copyright (c) Cognitect, Inc. All rights reserved.

(ns ^{:doc "Impl details for datomic.aws.navify. Not a public API.
Subject to change."}
    datomic.aws.navify.impl
  (:require
   [clojure.string :as str]
   [cognitect.aws.client.api :as aws]))

(defn arn?
  "Returns true if s is an arn string."
  [s]
  (and (string? s) (str/starts-with? s "arn:")))

(defn parse-arn
  "Parses arn into its constituent parts, returning them as a map with the keys:
  :arn :partition, :service, :region, :account, :resource, :resource-type.

   See Also:
   https://docs.aws.amazon.com/IAM/latest/UserGuide/reference-arns.html"
  [arn]
  (let [{:keys [resource-suffix] :as parsed} (zipmap [:arn
                                                      :partition
                                                      :service
                                                      :region
                                                      :account
                                                      :resource-suffix]
                                                     (str/split arn #":" 6))
        [resource-or-type maybe-resource] (str/split resource-suffix #":|/" 2)]
    (merge (dissoc parsed :resource-suffix)
           (if maybe-resource
             {:resource-type resource-or-type
              :resource maybe-resource}
             {:resource resource-or-type}))))

(defn with-nav
  "Add nav-fn as nav to x."
  [x nav-fn]
  (when x
    (vary-meta x assoc 'clojure.core.protocols/nav nav-fn)))

(defn nav-all-vs
  "nav-fn that applies f to all vs. Recursive wherever (f v) is a collection."
  [f]
  (fn [c k v]
    #_(tap> {:c c :k k :v v})
    (let [result (f v)]
      (if (coll? result)
        (with-nav result (nav-all-vs f))
        result))))

(defn resource-type-arn
  "Returns string arn for a parsed arn.
  delim may need to be : or / depending on resource.
  See https://docs.aws.amazon.com/IAM/latest/UserGuide/reference-arns.html"
  [{:keys [partition service region account resource resource-type]
    :or {partition "" service "" account "" region "" resource ""}} delim]
  (if resource-type
    (format "arn:%s:%s:%s:%s:%s%s%s"
            partition service region account resource-type delim resource)
    (format "arn:%s:%s:%s:%s:%s"
            partition service region account resource)))

(defmulti describe-args-by-resource-type
  "Helper for describe args for services that have a categoric
   resource-type."
  (fn [{:keys [service resource-type]}] [service resource-type]))

(defmethod describe-args-by-resource-type :default [_] nil)

(defmethod describe-args-by-resource-type  ["sns" nil]
  [parsed-arn]
  {:op :GetTopicAttributes
   :request {:TopicArn (resource-type-arn parsed-arn ":")}})

(defmethod describe-args-by-resource-type  ["ec2" "instance"]
  [{:keys [resource]}]
  {:op :DescribeInstances
   :request {:InstanceIds [resource]}})

(defmethod describe-args-by-resource-type  ["ec2" "subnet"]
  [{:keys [resource]}]
  {:op :DescribeSubnets
   :request {:SubnetIds [resource]}})

(defmethod describe-args-by-resource-type  ["ec2" "vpc"]
  [{:keys [resource]}]
  {:op :DescribeVpcs
   :request {:VpcIds [resource]}})

(defmethod describe-args-by-resource-type  ["ec2" "internet-gateway"]
  [{:keys [resource]}]
  {:op :DescribeInternetGateways
   :request {:InternetGatewayIds [resource]}})

(defmethod describe-args-by-resource-type  ["ec2" "security-group"]
  [{:keys [resource]}]
  {:op :DescribeSecurityGroups
   :request {:GroupIds [resource]}})

(defmethod describe-args-by-resource-type  ["dynamodb" "table"]
  [{:keys [resource]}]
  {:op :DescribeTable
   :request {:TableName resource}})

(defmethod describe-args-by-resource-type  ["codebuild" "build"]
  [{:keys [resource]}]
  {:op :BatchGetBuilds
   :request {:ids [resource]}})

;; does not handle aliases and versions yet
(defmethod describe-args-by-resource-type  ["lambda" "function"]
  [{:keys [resource]}]
  {:op :GetFunction
   :request {:FunctionName resource}})

(defmethod describe-args-by-resource-type  ["elasticloadbalancing" "loadbalancer"]
  [{:keys [resource]}]
  {:op :DescribeLoadBalancers
   :request {:LoadBalancerNames [resource]}})

(defmethod describe-args-by-resource-type  ["cloudformation" "stack"]
  [parsed-arn]
  {:op :DescribeStacks
   ;; have to specify full arn, not all resource names match validation regex
   :request {:StackName (resource-type-arn parsed-arn "/")}})

(defmethod describe-args-by-resource-type  ["states" "stateMachine"]
  [parsed-arn]
  {:op :DescribeStateMachine
   :request {:stateMachineArn (resource-type-arn parsed-arn ":")}})

(defmethod describe-args-by-resource-type  ["iam" "role"]
  [{:keys [resource]}]
  {:op :GetRole
   :request {:RoleName resource}})

;; v1 arn:${Partition}:elasticloadbalancing:${Region}:${Account}:loadbalancer/${LoadBalancerName}	
;; app arn:${Partition}:elasticloadbalancing:${Region}:${Account}:loadbalancer/app/${LoadBalancerName}/${LoadBalancerId)
;; net arn:${Partition}:elasticloadbalancing:${Region}:${Account}:loadbalancer/net/${LoadBalancerName}/${LoadBalancerId}
(defmethod describe-args-by-resource-type  ["elasticloadbalancing" "loadbalancer"]
  [{:keys [resource] :as parsed-arn}]
  (if
    (or (str/starts-with? resource "app/")
        (str/starts-with? resource "net/"))
    {:op :DescribeLoadBalancers
     :request {:LoadBalancerArns [(resource-type-arn parsed-arn "/")]}}
    {:op :DescribeLoadBalancers
     :request {:LoadBalancerNames [resource]}}))

(defmethod describe-args-by-resource-type  ["elasticloadbalancing" "targetgroup"]
  [{:keys [resource] :as parsed-arn}]
  {:op :DescribeTargetGroups
   :request {:TargetGroupArns [(resource-type-arn parsed-arn "/")]}})

(defmulti describe-api-gateway-args
  "API Gateway resource types are /-delimited inside the resource string."
  (fn [parsed-arn [e1 _ e3 :as elems]] [e1 e3 (count elems)]))

(defmethod describe-api-gateway-args :default [_ _] nil)

(defmethod describe-api-gateway-args ["apis" nil 2]
  [_ [_ api-id]]
  {:op :GetApi
   :request {:ApiId api-id}})

(defmethod describe-api-gateway-args ["apis" "stages" 4]
  [_ [_ api-id _ stage-name]]
  {:op :GetStage
   :request {:ApiId api-id :StageName stage-name}})

(defmethod describe-api-gateway-args ["vpclinks" nil 2]
  [_ [_ vpc-link-id]]
  {:op :GetVpcLink
   :request {:VpcLinkId vpc-link-id}})

;; apigateway has an empty resource type string and
;; multiple resource types delimited by slashes in the resource value
;; https://docs.aws.amazon.com/service-authorization/latest/reference/list_amazonapigatewaymanagementv2.html
(defmethod describe-args-by-resource-type  ["apigateway" ""]
  [{:keys [resource] :as parsed-arn}]
  (describe-api-gateway-args parsed-arn (str/split resource #"/")))

(defmulti describe-args
  "Given a parsed arn, return the args to make an AWS API call to describe
  the resource, or nil."
  :service)

(defmethod describe-args :default
  [arg]
  (describe-args-by-resource-type arg))

;; only handling buckets/items for now
;; https://docs.aws.amazon.com/service-authorization/latest/reference/list_amazons3.html
(defmethod describe-args "s3"
  [{:keys [resource resource-type]}]
  (if resource
    {:op :HeadObject
     :request {:Bucket resource-type
               :Key resource}}
    {:op :HeadBucket
     :request {:Bucket resource-type}}))

(defmulti client
  "Returns an AWS API client for parsed arn."
  :service)

;; Some services have different client versions for the same resource-type.
(defmethod client "elasticloadbalancing"
  [{:keys [resource-type resource]}]
  (if (and (= resource-type "loadbalancer")
           (not (or (str/starts-with? resource "app/")
                    (str/starts-with? resource "net/"))))
    (aws/client {:api :elasticloadbalancing})
    (aws/client {:api :elasticloadbalancingv2})))

;; Some service client names do not match the ARN service key.
(defmethod client "apigateway"
  [{:keys [resource-type resource]}]
  (aws/client {:api :apigatewayv2}))

(defmethod client :default
  [{:keys [service]}]
  (aws/client {:api (keyword service)}))

(defn maybe-transform
  "Maybe transform x

  pred        should transform?
  df          datafy fn
  ef          (fn [x thrown]) to return if df throws"
  [x pred df ef]
  (if (pred x)
    (try
      (df x)
      (catch Throwable t (ef x t)))
    x))

(defn describe-arn
  [arn]
  (let [parsed (parse-arn arn)
        client (client parsed)
        args (describe-args parsed)]
    (aws/invoke client args)))

(defn transform-describing-arn
  "If x is an arn, try to describe it, else return x."
  [x]
  (maybe-transform x arn? describe-arn (fn [x t] {:arn x :describe-threw t})))

(defmulti console-url-by-resource-type
  "Helper for console urls for services that have a categoric
   resource-type."
  (fn [{:keys [service resource-type]}] [service resource-type]))

(defmethod console-url-by-resource-type :default [_] nil)

(defmethod console-url-by-resource-type ["ec2" "instance"]
  [{:keys [region resource]}]
  (format  "https://%s.console.aws.amazon.com/ec2/home?region=%s#InstanceDetails:instanceId=%s"
           region region resource))

(defn console-url
  "Given a parsed-arn, return the AWS console URL to browse that ARN, or nil."
  [arn]
  (some-> (console-url-by-resource-type arn)
          (java.net.URL.)))

(defn transform-browsing-arn
  "If x is an arn, try to return a browse URL."
  [x]
  (maybe-transform x arn? (comp console-url parse-arn) (fn [x t] {:arn x :browse-url-threw t})))
