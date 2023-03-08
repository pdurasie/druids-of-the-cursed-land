import 'package:flutter/material.dart';
import 'package:druids_of_the_cursed_land/presentation/widgets/home.dart';

void main() {
  runApp(const MyApp());
}

class MyApp extends StatelessWidget {
  const MyApp({super.key});

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'Flutter Demo',
      theme: ThemeData(
        primarySwatch: Colors.blue,
      ),
      home: const MapSample(),
    );
  }
}

/*
THE GOAL
More than anything, I want a map that one can collect herbs from in real life.

NECESSARY
Spawn some items and store their location in a backend
  - Use OpenStreetMap data: Biomes spawn different herbs, only spawn in areas where
    foot=yes (accessible by foot)
  - Every 20 minutes, the spawning must operate again
  - Maybe do one per geocode for now

  172.17.0.1
 */
