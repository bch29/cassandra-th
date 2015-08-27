{-# LANGUAGE CPP #-}
module Cassandra.RecordTH (recordInstanceFromDec) where

import Control.Monad
import Language.Haskell.TH
import Language.Haskell.TH.Syntax

import Database.CQL.Protocol

typeSynDecl :: Name -> [Type] -> Type -> Dec
#if __GLASGOW_HASKELL__ < 708
typeSynDecl = TySynInstD
#else
typeSynDecl x y z = TySynInstD x (TySynEqn y z)
#endif

recordInstanceFromDec :: Dec -> Q [Dec]
recordInstanceFromDec (DataD _ tname _ cons _) = do
    unless (length cons == 1) $
        fail "expecting single data constructor"
    let tt = tupleType (head cons)
    at <- asTupleDecl (head cons)
    ar <- asRecrdDecl (head cons)
    return
        [ typeSynDecl (mkName "TupleType") [ConT tname] tt
        , InstanceD [] (ConT (mkName "Record") $: ConT tname)
            [ FunD (mkName "asTuple")  [at]
            , FunD (mkName "asRecord") [ar]
            ]
        ]
start _ = fail "unsupported data type"

tupleType :: Con -> Type
tupleType c =
    let tt = types c
    in foldl1 ($:) (TupleT (length tt) : types c)
  where
    types (NormalC _ tt) = map snd tt
    types (RecC _ tt)    = map (\(_, _, t) -> t) tt
    types _              = fail "record and normal constructors only"

asTupleDecl ::Con -> Q Clause
asTupleDecl c =
    case c of
        (NormalC n t) -> go n t
        (RecC    n t) -> go n t
        _             -> fail "record and normal constructors only"
  where
    go n t = do
        vars <- replicateM (length t) (newName "a")
        return $ Clause [ConP n (map VarP vars)] (body vars) []
    body = NormalB . TupE . map VarE

asRecrdDecl ::Con -> Q Clause
asRecrdDecl c =
    case c of
        (NormalC n t) -> go n t
        (RecC    n t) -> go n t
        _             -> fail "record and normal constructors only"
  where
    go n t = do
        vars <- replicateM (length t) (newName "a")
        return $ Clause [TupP (map VarP vars)] (body n vars) []
    body n v = NormalB $ foldl1 ($$) (ConE n : map VarE v)

($$) :: Exp -> Exp -> Exp
($$) = AppE

($:) :: Type -> Type -> Type
($:) = AppT
