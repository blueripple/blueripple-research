{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}

module Stan.ModelBuilder.DesignMatrix where

import Prelude hiding (All)
--import qualified Stan.ModelBuilder.Distributions as SD
import qualified Stan.ModelBuilder as SB

import qualified Control.Foldl as FL
import qualified Control.Scanl as SL
import Data.Functor.Contravariant (Contravariant(..))
import qualified Data.Array as Array
import qualified Data.Map as Map
import qualified Data.Set as Set
import qualified Data.Vector.Unboxed as V
import Streamly.Internal.Data.SVar (dumpSVar)
--import qualified Data.Dependent.HashMap as DHash
--import qualified Data.Dependent.Sum as DSum
--import qualified Stan.ModelBuilder.SumToZero as STZ
--import Stan.ModelBuilder.SumToZero (SumToZero(..))

--data NEGroupInfo r k = HGroupInfo (r -> V.Vector Double)
--newtype NEGroups r = HGroups DHash.DHashMap SB.GroupTypeTag

data DesignMatrixRowPart r = DesignMatrixRowPart { dmrpName :: Text
                                                 , dmrpLength :: Int
                                                 , dmrpVecF :: r -> V.Vector Double
                                                 }


instance Contravariant DesignMatrixRowPart where
  contramap g (DesignMatrixRowPart n l f) = DesignMatrixRowPart n l (f . g)

data DesignMatrixRow r = DesignMatrixRow { dmName :: Text
                                         , dmParts :: [DesignMatrixRowPart r]
                                         }

instance Contravariant DesignMatrixRow where
  contramap g (DesignMatrixRow n dmrps) = DesignMatrixRow n $ fmap (contramap g) dmrps


rowLengthF :: FL.Fold (DesignMatrixRowPart r) Int
rowLengthF = FL.premap dmrpLength FL.sum

rowFuncF :: FL.Fold (DesignMatrixRowPart r) (r -> V.Vector Double)
rowFuncF = appConcat . sequenceA <$> FL.premap dmrpVecF FL.list
  where appConcat g r = V.concat (g r)

matrixFromRowData :: DesignMatrixRow r -> SB.MatrixRowFromData r
matrixFromRowData (DesignMatrixRow name rowParts) = SB.MatrixRowFromData name length f
  where (length, f) = FL.fold ((,) <$> rowLengthF <*> rowFuncF) rowParts

{-
combineRowFuncs :: Foldable f => f (Int, r -> V.Vector Double) -> (Int, r -> V.Vector Double)
combineRowFuncs rFuncs =
  let nF = FL.premap fst FL.sum
      fF = (\r -> V.concat . fmap ($r)) <$> FL.premap snd FL.list
  in FL.fold ((,) <$> nF <*> fF) rFuncs
-}

boundedEnumRowFunc :: forall k r.(Enum k, Bounded k, Eq k) => (r -> k) -> (Int, r -> V.Vector Double)
boundedEnumRowFunc rToKey = case numKeys of
  1 -> error "Single element enum given to boundedEnumRowFunc"
  2 -> binary
  _ -> nonBinary
  where
    keys :: [k] = universe
    numKeys = length keys
    binary = (1, \r -> V.singleton $ realToFrac $ if rToKey r == minBound then -1 else 1)
    oneZero r x = if rToKey r == x then 1 else 0
    nonBinary = (numKeys, \r -> V.fromList $ fmap (oneZero r) keys)

-- adds matrix (name_dataSetName)
-- adds K_name for col dimension (also <NamedDim name_Cols>)
-- row dimension should be N_dataSetNamer  (which is <NamedDim dataSetName)
-- E.g., if name="Design" and dataSetName="myDat"
-- In data
-- "Int N_myDat;" (was already there)
-- "Int K_Design;"
-- "matrix[N_myDat, K_Design] Design_myDat;"
-- with accompanying json
addDesignMatrix :: (Typeable md, Typeable gq) => SB.RowTypeTag r -> DesignMatrixRow r -> SB.StanBuilderM md gq SB.StanVar
addDesignMatrix rtt dmr = SB.add2dMatrixJson rtt (matrixFromRowData dmr) ""  (SB.NamedDim (SB.dataSetName rtt))

designMatrixIndexes :: DesignMatrixRow r -> [(Text, Int, Int)]
designMatrixIndexes (DesignMatrixRow _ dmps)= SL.scan rowPartScan dmps where
  rowPartScanStep rp = do
    curIndex <- get
    put $ curIndex + dmrpLength rp
    return (dmrpName rp, dmrpLength rp, curIndex)
  rowPartScan = SL.Scan rowPartScanStep 1

-- adds J_Group and Group_Design_Index for all parts of row
addDesignMatrixIndexes :: (Typeable md, Typeable gq) => DesignMatrixRow r -> SB.StanBuilderM md gq ()
addDesignMatrixIndexes dmr = do
  let addEach (gName, gSize, gStart) = do
        sv <- SB.addFixedIntJson ("J_" <> gName) Nothing gSize
        SB.addDeclBinding gName sv
        SB.addFixedIntJson (gName <> "_" <> dmName dmr <> "_Index") Nothing gStart
        return ()
  traverse_ addEach $ designMatrixIndexes dmr

-- we assume we've already checked the dimension
splitToGroupVar :: Text -> Text -> SB.StanVar -> SB.StanBuilderM md gq SB.StanVar
splitToGroupVar dName gName v@(SB.StanVar n st) = do
  let newVarName = n <> "_" <> gName
      index = gName <> "_" <> dName <> "_Index"
      namedDimE x = SB.stanDimToExpr $ SB.NamedDim x
      segment x = SB.function "segment" $ SB.var x :| [SB.name index, namedDimE gName]
      block d x = SB.function "block" $ SB.var x :| [SB.scalar "1", SB.name index, namedDimE d, namedDimE gName]
  case st of
    SB.StanVector _ -> SB.stanDeclareRHS newVarName (SB.StanVector $ SB.NamedDim gName) "" $ segment v
    SB.StanArray [SB.NamedDim d] (SB.StanVector _) -> do
      nv <- SB.stanDeclare newVarName (SB.StanArray [SB.NamedDim d] $ SB.StanVector $ SB.NamedDim gName) ""
      SB.stanForLoopB "k" Nothing d $ SB.addExprLine "splitToGroupVec" $ SB.var nv `SB.eq` segment v
      return nv
    SB.StanMatrix (SB.NamedDim d, _) -> SB.stanDeclareRHS newVarName (SB.StanMatrix (SB.NamedDim d, SB.NamedDim gName)) "" $ block d v
    _ -> SB.stanBuildError "DesignMatrix.splitToGroupVar: Can only split vectors, arrays of vectors or matrices. And the latter, only with named row dimension."

-- take a stan vector, array, or matrix indexed by this design row
-- and split into the parts for each group
-- this doesn't depend on r
splitToGroupVars :: DesignMatrixRow r -> SB.StanVar -> SB.StanBuilderM md gq [SB.StanVar]
splitToGroupVars dmr@(DesignMatrixRow n _) v@(SB.StanVar _ st) = do
  let designColName = "K_" <> n
  case st of
    SB.StanVector d -> when (d /= SB.NamedDim designColName)
      $ SB.stanBuildError $ "DesignMatrix.splitTogroupVars: vector to split has wrong dimension: " <> show d
    SB.StanArray _ (SB.StanVector d) -> when (d /= SB.NamedDim designColName)
      $ SB.stanBuildError $ "DesignMatrix.splitTogroupVars: vectors in array of vectors to split has wrong dimension: " <> show d
    SB.StanMatrix (d, _)  -> when (d /= SB.NamedDim designColName)
      $ SB.stanBuildError $ "DesignMatrix.splitTogroupVars: matrix to split has wrong row-dimension: " <> show d
  traverse (\(g, _, _) -> splitToGroupVar n g v) $ designMatrixIndexes dmr
