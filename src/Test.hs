{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Test where

import Cassandra.QQ
import Cassandra.TH

import Cassandra
import Database.CQL.Protocol

newtype UserId = UserId Int32
               deriving (Cql, Show)

makeSchema (Just "output.cql") "test" [schema|
-- > typesyn a b
-- Makes 'a' a synonym for 'b'
typesyn user_id int

typesyn word text

-- typemap a B
-- Maps the CQL type 'a' to the Haskell type 'B', which must have a Cql instance
-- Works on synonyms
typemap user_id UserId

typesyn name word

udt full_name
  first_name name
  last_name name
end

udt user_info
  user_id user_id
  name full_name
  alias name
end

table test1 (id)
  id int
  name full_name
  user_id user_id
end

table test2 ((id1, id2), blah)
  id1 int
  id2 int
  blah text
end

table test3 (id, blah, blegh)
  id int
  blah int
  blegh uuid
end

view test1_view (test1)
  id
  name
end
|]
