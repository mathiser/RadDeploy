for fol in base storescp fingerprinter consumer storescu flow_tracker
do
  cd $fol
  bash build.sh
  cd ..
done