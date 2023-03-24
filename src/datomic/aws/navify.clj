;; Copyright (c) Cognitect, Inc. All rights reserved.

(ns datomic.aws.navify
  (:require
   [clojure.string :as str]
   [cognitect.aws.client.api :as aws]
   [datomic.aws.navify.impl :as impl]))

(defn navify-arn
  "Navify arn for viewing in a nav-aware tool. Not an API for programs,
  data returned is subject to change. Currently attempts to use the
  Cognitect AWS API to call an appropriate 'describe' for the resource."
  [arn]
  (impl/with-nav
    (impl/transform-describing-arn arn)
    (impl/nav-all-vs impl/transform-describing-arn)))



